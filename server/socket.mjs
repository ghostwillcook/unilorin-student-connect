/**
 * UNILORIN Student Connect — standalone realtime server.
 *
 * Socket.io needs a long-lived connection, which Vercel's serverless functions
 * cannot hold, so the realtime layer is its own process: `npm run socket`.
 * That independence is the source of most of the decisions in this file.
 *
 *   - It gets no help from Next: no `@/` alias, no automatic .env.local load,
 *     and no access to anything in lib/ (those are TypeScript). The small
 *     duplications below — env parsing, settings reading, the DM wire shape —
 *     are deliberate copies of lib/ logic, kept behaviourally identical rather
 *     than imported.
 *   - It shares exactly one thing with the web app: NEXTAUTH_SECRET. Clients
 *     hand over the raw NextAuth session token they fetched from
 *     /api/socket-token and this process decrypts it. No second credential
 *     system, no session table.
 *
 * Wire contract (mirrored by lib/socket-client.ts):
 *
 *   in    chat:send             { content }
 *         chat:history:request  {}
 *         complaint:join        { complaintId }
 *         complaint:send        { complaintId, content }
 *         complaint:delete      { messageId }
 *         livechat:join         { conversationId? }
 *         livechat:send         { content }
 *         livechat:reply        { conversationId, content }
 *         livechat:delete       { messageId }
 *         notification:send     { title, body, userId? }   ADMIN
 *   out   chat:history          PublicChatMessage[]   last 50, oldest first
 *         chat:message          PublicChatMessage     broadcast to everyone
 *         chat:error            { message }           to the one sender
 *         complaint:new         ComplaintMessage      to complaint:<id>
 *         complaint:deleted     { id }                to complaint:<id>
 *         livechat:new          LiveMessage           to live:<conversationId>
 *         livechat:deleted      { id }                to live:<conversationId>
 *         notification:new      { id, title, body, createdAt }  to user:<id>
 *         notification:sent     { count }             to the sending admin
 *         presence              { onlineStudents, onlineAdmins }
 *         session               { pseudonym }         on connect
 *
 * dm:send/dm:new are gone: the DM channel merged into Live Chat ("Messages")
 * — DirectMessage's rows live in LiveMessage since migration
 * 20260829100000_merge_dm_livechat.
 *
 * The message families are thin realtime skins over REST-backed tables:
 * ComplaintMessage and LiveMessage. Every socket write is a plain
 * prisma.*.create — the same rows /api/complaints/[id]/messages and
 * /api/livechat write — so the database is the source of truth and a socket
 * that is down only costs the instant echo, never the message. Authorization
 * is per-room and per-write: joining complaint:<id> requires owning the
 * complaint or being an admin, and a student's livechat:send can only ever
 * address their own conversation because the conversation row is looked up BY
 * their user id, never by a client-supplied conversation id. Deletion is the
 * same rule per table: students remove their own messages, admins any.
 *
 * PublicChatMessage carries no userId, ever. That is enforced here by the
 * `select` on every ChatMessage query rather than by remembering to strip a
 * field before emitting.
 */

import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Server } from "socket.io";
import { PrismaClient } from "@prisma/client";
import { decode } from "next-auth/jwt";
import { notifyNewAdminMessage } from "../lib/notify-student.mjs";

/* ------------------------------------------------------------------ env load */

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Minimal .env reader.
 *
 * `dotenv` is not a dependency and this process starts outside Next, which is
 * what normally loads .env.local. Without this the socket server would come up
 * with no DATABASE_URL and no NEXTAUTH_SECRET and reject every handshake, while
 * `npm run dev` alongside it worked fine — a genuinely confusing failure.
 *
 * Real environment variables always win, so a deployment that injects config
 * (Railway, Fly.io) is never overridden by a stray checked-out file. Values may
 * be quoted; unquoted values may carry a trailing `# comment`. Multi-line
 * values are not supported — nothing this app needs uses them.
 */
function loadEnvFile(filename) {
  let raw;
  try {
    raw = readFileSync(resolve(ROOT, filename), "utf8");
  } catch {
    // Absent is the normal case for at least one of the two files.
    return;
  }

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;

    const body = trimmed.startsWith("export ")
      ? trimmed.slice("export ".length).trim()
      : trimmed;

    const eq = body.indexOf("=");
    if (eq <= 0) continue;

    const key = body.slice(0, eq).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue;
    if (process.env[key] !== undefined) continue;

    process.env[key] = cleanValue(body.slice(eq + 1).trim());
  }
}

function cleanValue(input) {
  const quote = input.charAt(0);
  const isQuoted =
    input.length >= 2 &&
    (quote === '"' || quote === "'") &&
    input.endsWith(quote);

  if (isQuoted) {
    const inner = input.slice(1, -1);
    // Only double quotes get escape expansion, matching dotenv. A `#` inside
    // quotes is data — Postgres passwords legitimately contain one.
    return quote === '"' ? inner.replace(/\\n/g, "\n").replace(/\\r/g, "\r") : inner;
  }

  const comment = input.indexOf(" #");
  return (comment === -1 ? input : input.slice(0, comment)).trim();
}

// .env.local first: it is the file the README tells people to create, and the
// `!== undefined` guard above means whichever is read first wins.
loadEnvFile(".env.local");
loadEnvFile(".env");

/* -------------------------------------------------------------------- config */

/**
 * SOCKET_PORT stays authoritative so a local override always wins, but hosted
 * platforms (Render, Railway, Fly) inject PORT and require the process to bind
 * exactly that — a service listening on 4000 while the platform routes to PORT
 * fails its health check and is killed as unresponsive.
 */
const PORT = Number(process.env.SOCKET_PORT || process.env.PORT || 4000);
const ORIGIN = process.env.NEXTAUTH_URL || "http://localhost:3000";
const SECRET =
  process.env.NEXTAUTH_SECRET || "dev-only-insecure-secret-change-me";

const HISTORY_LIMIT = 50;
const MAX_CHAT_LENGTH = 2000;
/** Matches the REST routes for complaint threads and Live Chat. */
const MAX_THREAD_LENGTH = 4000;
const RATE_WINDOW_MS = 60_000;
const SETTINGS_TTL_MS = 5_000;
/** Admins are never anonymised in the global room — see connection handler. */
const STAFF_LABEL = "Student Affairs";

if (!process.env.DATABASE_URL) {
  console.error(
    [
      "",
      "  Cannot start the realtime server: DATABASE_URL is not set.",
      "",
      "  Chat and direct messages are persisted, so there is nothing useful to",
      "  do without a database. Copy .env.local.example to .env.local, fill in",
      "  your Supabase connection strings, then run:",
      "",
      "      npx prisma migrate dev",
      "      npm run socket",
      "",
    ].join("\n"),
  );
  process.exit(1);
}

if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
  console.error(
    `  Cannot start the realtime server: SOCKET_PORT="${process.env.SOCKET_PORT}" is not a valid port.`,
  );
  process.exit(1);
}

if (!process.env.NEXTAUTH_SECRET) {
  // In production the dev fallback is a forged-token hole, not a convenience:
  // the fallback string is public (it ships in this repo), so anyone can mint
  // a token it accepts. The web app refuses the fallback in production, and
  // this process must too — a misconfigured deploy should fail loudly at
  // boot, not authenticate an attacker until someone notices.
  if (process.env.NODE_ENV === "production") {
    console.error(
      "  [socket] NEXTAUTH_SECRET is not set in production — refusing to start\n" +
        "           with the insecure dev fallback. Set the secret (identical to\n" +
        "           the web app's) and redeploy.",
    );
    process.exit(1);
  }
  console.warn(
    "  [socket] NEXTAUTH_SECRET is not set — falling back to the insecure dev\n" +
      "           secret. Handshakes only succeed while the web app uses the same\n" +
      "           fallback. Set it in both processes before deploying.",
  );
}

const prisma = new PrismaClient({ log: ["error"] });

function errText(error) {
  if (error instanceof Error) return error.message;
  return String(error);
}

/* ------------------------------------------------------------------ settings */

const DEFAULT_SETTINGS = {
  anonymousMode: true,
  chatRateLimitPerMin: 20,
  complaintSubmissionLimit: 0,
};

/**
 * Cached mirror of lib/settings.ts, parsed identically (`"true"` is the only
 * truthy spelling; a non-positive integer falls back to the default).
 *
 * Cached for a few seconds because chat:send would otherwise pay a round trip
 * per message. The trade is that flipping a setting in /admin/settings takes
 * effect within SETTINGS_TTL_MS instead of instantly, which is invisible to a
 * human but keeps a busy room from hammering the Setting table.
 */
let settingsCache = { value: { ...DEFAULT_SETTINGS }, readAt: 0 };

async function getSettings() {
  const now = Date.now();
  if (now - settingsCache.readAt < SETTINGS_TTL_MS) return settingsCache.value;

  try {
    const rows = await prisma.setting.findMany();
    const map = new Map(rows.map((row) => [row.key, row.value]));

    const anonymousRaw = map.get("anonymousMode");
    const limitRaw = Number.parseInt(map.get("chatRateLimitPerMin") ?? "", 10);
    const complaintLimitRaw = Number.parseInt(
      map.get("complaintSubmissionLimit") ?? "",
      10,
    );

    settingsCache = {
      value: {
        anonymousMode:
          anonymousRaw === undefined
            ? DEFAULT_SETTINGS.anonymousMode
            : anonymousRaw === "true",
        chatRateLimitPerMin:
          Number.isFinite(limitRaw) && limitRaw > 0
            ? limitRaw
            : DEFAULT_SETTINGS.chatRateLimitPerMin,
        complaintSubmissionLimit:
          Number.isFinite(complaintLimitRaw) && complaintLimitRaw >= 0
            ? complaintLimitRaw
            : DEFAULT_SETTINGS.complaintSubmissionLimit,
      },
      readAt: now,
    };
  } catch (error) {
    // The table may not exist yet (pre-migration). Defaults keep chat working.
    console.warn("[socket] settings read failed, using defaults:", errText(error));
    settingsCache = { value: { ...DEFAULT_SETTINGS }, readAt: now };
  }

  return settingsCache.value;
}

/* ---------------------------------------------------------------- rate limit */

/**
 * Sliding window per userId, held in memory.
 *
 * Per *user* rather than per socket, so opening a second tab does not double
 * anyone's allowance. In-memory is the right scope: a single socket process
 * sees every message, and a restart forgiving old hits is harmless.
 *
 * @type {Map<string, number[]>}
 */
const rateHits = new Map();

function checkRateLimit(userId, limitPerMin) {
  const now = Date.now();
  const cutoff = now - RATE_WINDOW_MS;
  const hits = (rateHits.get(userId) ?? []).filter((at) => at > cutoff);

  if (hits.length >= limitPerMin) {
    rateHits.set(userId, hits);
    // The oldest hit in the window is the one that has to age out.
    const oldest = hits[0] ?? now;
    const waitMs = oldest + RATE_WINDOW_MS - now;
    return { ok: false, retryInSeconds: Math.max(1, Math.ceil(waitMs / 1000)) };
  }

  hits.push(now);
  rateHits.set(userId, hits);
  return { ok: true, retryInSeconds: 0 };
}

// Without this, every user who ever connected keeps an array forever.
const pruneTimer = setInterval(
  () => {
    const cutoff = Date.now() - RATE_WINDOW_MS;
    for (const [userId, hits] of rateHits) {
      const fresh = hits.filter((at) => at > cutoff);
      if (fresh.length === 0) rateHits.delete(userId);
      else rateHits.set(userId, fresh);
    }
  },
  5 * 60_000,
);
pruneTimer.unref();

/* ------------------------------------------------------------------ presence */

/**
 * Distinct users online, each holding the socket ids it has open.
 *
 * Keyed by userId so two tabs count once — presence answers "how many people
 * are here", not "how many connections exist".
 *
 * @type {Map<string, { role: "STUDENT" | "ADMIN", sockets: Set<string> }>}
 */
const online = new Map();

function addPresence(user, socketId) {
  const entry = online.get(user.id);
  if (entry) entry.sockets.add(socketId);
  else online.set(user.id, { role: user.role, sockets: new Set([socketId]) });
}

function removePresence(userId, socketId) {
  const entry = online.get(userId);
  if (!entry) return;
  entry.sockets.delete(socketId);
  if (entry.sockets.size === 0) online.delete(userId);
}

function presencePayload() {
  let onlineStudents = 0;
  let onlineAdmins = 0;
  for (const entry of online.values()) {
    if (entry.role === "ADMIN") onlineAdmins += 1;
    else onlineStudents += 1;
  }
  return { onlineStudents, onlineAdmins };
}

/* ----------------------------------------------------------------- pseudonym */

/**
 * Pseudonyms currently in use, so two students in the same room are not both
 * "Anonymous #42" — which would read as one person contradicting themselves.
 *
 * Fallback-only since the global room switched to reusing each student's
 * LiveConversation pseudonym (see the connection handler): the in-process set
 * guards only the random claims made when that lookup fails.
 *
 * @type {Set<string>}
 */
const takenPseudonyms = new Set();

/**
 * Claims a random pseudonym for this connection. Fallback-only — see
 * takenPseudonyms above for when this runs.
 */
function claimPseudonym() {
  // 10..999 keeps N at the specified 2-3 digits.
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const candidate = `Anonymous #${10 + Math.floor(Math.random() * 990)}`;
    if (!takenPseudonyms.has(candidate)) {
      takenPseudonyms.add(candidate);
      return candidate;
    }
  }
  // 20 collisions means the room is implausibly full; a duplicate beats a hang.
  return `Anonymous #${10 + Math.floor(Math.random() * 990)}`;
}

function releasePseudonym(pseudonym) {
  if (pseudonym) takenPseudonyms.delete(pseudonym);
}

/**
 * The name to attach to a message right now.
 *
 * The pseudonym itself is fixed for the life of the connection, but whether it
 * is used is read fresh: an admin turning anonymousMode off should not have to
 * ask every student to reconnect.
 */
async function displayNameFor(socket) {
  const { anonymousMode } = await getSettings();
  if (anonymousMode) return socket.data.pseudonym;
  return socket.data.user.name || socket.data.pseudonym;
}

/* -------------------------------------------------------------- wire helpers */

const CHAT_PUBLIC_FIELDS = {
  id: true,
  pseudonym: true,
  content: true,
  timestamp: true,
};

function toPublicChatMessage(row) {
  return {
    id: row.id,
    pseudonym: row.pseudonym,
    content: row.content,
    timestamp: row.timestamp.toISOString(),
  };
}

/**
 * Pulls `content` off an event payload.
 *
 * Returns null for anything unusable, so callers have one thing to check. A
 * bare string is tolerated as well as the contract's `{ content }`.
 */
function readContent(payload) {
  const raw =
    typeof payload === "string"
      ? payload
      : payload && typeof payload === "object" && !Array.isArray(payload)
        ? payload.content
        : undefined;

  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : null;
}

async function recentHistory() {
  // Newest 50 by query, then reversed: the client renders oldest first.
  const rows = await prisma.chatMessage.findMany({
    orderBy: [{ timestamp: "desc" }, { id: "desc" }],
    take: HISTORY_LIMIT,
    select: CHAT_PUBLIC_FIELDS,
  });
  return rows.reverse().map(toPublicChatMessage);
}

/* ----------------------------------------------------------------- transport */

const httpServer = createServer(async (req, res) => {
  // A plain HTTP probe for the platforms this is deployed to, and a quick way
  // to tell "server down" from "handshake refused" while debugging.
  //
  // This checks the DATABASE as well as the process — deliberately. The probe
  // used to report only uptime and in-memory presence, so a socket server that
  // could not reach Postgres answered 200 and looked perfectly healthy while
  // rejecting every single connection (the handshake verifies the user against
  // the database and fails closed). That is exactly what happened on
  // 2026-09-16: a stale DATABASE_URL password made the realtime feature dead
  // for however long it took someone to notice by hand, and this endpoint
  // reported green through all of it. A health check that cannot fail is not a
  // health check.
  //
  // 503 rather than a 200-with-a-flag: monitoring tools act on the status code,
  // and a socket server with no database cannot serve anyone.
  if (req.url === "/health" || req.url === "/healthz") {
    const startedAt = Date.now();
    let database = "connected";
    let databaseMs = 0;
    try {
      await prisma.$queryRawUnsafe("SELECT 1");
      databaseMs = Date.now() - startedAt;
    } catch (err) {
      database = "unreachable";
      databaseMs = Date.now() - startedAt;
      console.error(
        "[socket] health check cannot reach the database:",
        errText(err),
      );
    }

    res.writeHead(database === "connected" ? 200 : 503, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    });
    res.end(
      JSON.stringify({
        ok: database === "connected",
        uptimeSeconds: Math.round(process.uptime()),
        database,
        databaseMs,
        ...presencePayload(),
      }),
    );
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("UNILORIN Student Connect realtime server. Socket.io endpoint: /socket.io/\n");
});

const io = new Server(httpServer, {
  cors: { origin: ORIGIN, credentials: true },
});

function broadcastPresence() {
  io.emit("presence", presencePayload());
}

/* ---------------------------------------------------------------- handshake */

/**
 * Authenticates the connection from the NextAuth session token.
 *
 * Two checks, not one. The token says whether the account was active when it
 * was issued; the database says whether it is active *now*. Sessions last eight
 * hours, so trusting the token alone would let a student the Unit just blocked
 * keep talking for the rest of the day — which would quietly defeat the whole
 * point of the block button on /admin/users. One indexed lookup per connection
 * (not per message) is a cheap way to make deactivation immediate.
 */
io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth?.token;
    if (typeof token !== "string" || token.length === 0) {
      return next(new Error("unauthorized"));
    }

    let payload = null;
    try {
      payload = await decode({ token, secret: SECRET });
    } catch {
      // Tampered, expired, or signed with a different secret. All the same
      // answer: no detail goes back to the client.
      payload = null;
    }
    if (!payload || payload.isActive === false) {
      return next(new Error("unauthorized"));
    }

    const id =
      typeof payload.id === "string" && payload.id
        ? payload.id
        : typeof payload.sub === "string"
          ? payload.sub
          : "";
    if (!id) return next(new Error("unauthorized"));

    const user = await prisma.user.findUnique({
      where: { id },
      select: {
        id: true,
        name: true,
        role: true,
        studentId: true,
        isActive: true,
      },
    });
    if (!user || !user.isActive) return next(new Error("unauthorized"));

    // Role and name come from the row, not the claims: the database is the
    // authority on both, and a token cannot outrank it.
    socket.data.user = {
      id: user.id,
      role: user.role,
      name: user.name,
      studentId: user.studentId,
    };
    return next();
  } catch (error) {
    // Includes the database being unreachable. Failing closed is right: every
    // event handler needs the database anyway.
    console.error("[socket] handshake rejected:", errText(error));
    return next(new Error("unauthorized"));
  }
});

/* ------------------------------------------------------------------ handlers */

async function handleHistoryRequest(socket) {
  try {
    socket.emit("chat:history", await recentHistory());
  } catch (error) {
    console.error("[socket] chat:history failed:", errText(error));
    socket.emit("chat:error", {
      message: "Could not load earlier messages. Please try again.",
    });
  }
}

async function handleChatSend(socket, payload) {
  const user = socket.data.user;
  try {
    const content = readContent(payload);
    if (content === null) {
      socket.emit("chat:error", { message: "Message cannot be empty." });
      return;
    }
    if (content.length > MAX_CHAT_LENGTH) {
      socket.emit("chat:error", {
        message: `Message must be ${MAX_CHAT_LENGTH} characters or fewer.`,
      });
      return;
    }

    // Validation runs first so a rejected message never costs the sender part
    // of their allowance.
    const settings = await getSettings();
    const verdict = checkRateLimit(user.id, settings.chatRateLimitPerMin);
    if (!verdict.ok) {
      socket.emit("chat:error", {
        message: `You are sending messages too quickly. Try again in ${verdict.retryInSeconds}s.`,
      });
      return;
    }

    // The pseudonym is set in the connection bootstrap IIFE; a message sent
    // before that completes would otherwise be persisted without one. Claim a
    // random fallback (the in-process path) rather than write "undefined".
    if (socket.data.pseudonym === undefined) {
      socket.data.pseudonym = claimPseudonym();
    }

    const pseudonym = settings.anonymousMode
      ? socket.data.pseudonym
      : user.name || socket.data.pseudonym;

    // The real userId is persisted for the admin chat log; the `select` then
    // hands back only the four public fields, so the identity cannot reach the
    // broadcast even by accident.
    const row = await prisma.chatMessage.create({
      data: { userId: user.id, pseudonym, content },
      select: CHAT_PUBLIC_FIELDS,
    });

    io.emit("chat:message", toPublicChatMessage(row));
  } catch (error) {
    console.error("[socket] chat:send failed:", errText(error));
    socket.emit("chat:error", {
      message: "Message could not be delivered. Please try again.",
    });
  }
}

/* ------------------------------------------------- complaint thread events */

const COMPLAINT_MESSAGE_FIELDS = {
  id: true,
  complaintId: true,
  senderRole: true,
  content: true,
  createdAt: true,
};

function toComplaintMessage(row) {
  return {
    id: row.id,
    complaintId: row.complaintId,
    senderRole: row.senderRole,
    content: row.content,
    createdAt: row.createdAt.toISOString(),
  };
}

/**
 * Authorizes a complaint for this socket's user. Admins may open any thread;
 * a student only their own. Returns the complaint row or null — the caller
 * turns null into a quiet refusal, because "not yours" and "does not exist"
 * must not be distinguishable to a probing client.
 */
async function complaintForUser(user, complaintId) {
  if (typeof complaintId !== "string" || complaintId.trim().length === 0) {
    return null;
  }
  const where =
    user.role === "ADMIN"
      ? { id: complaintId }
      : { id: complaintId, userId: user.id };
  return prisma.complaint.findFirst({
    where,
    select: { id: true },
  });
}

async function handleComplaintJoin(socket, payload) {
  const user = socket.data.user;
  try {
    const complaintId =
      payload && typeof payload === "object" ? payload.complaintId : undefined;
    const complaint = await complaintForUser(user, complaintId);
    if (!complaint) {
      socket.emit("chat:error", { message: "Conversation not available." });
      return;
    }
    // join() is idempotent, so re-joining on every panel mount is free.
    await socket.join(`complaint:${complaint.id}`);
  } catch (error) {
    console.error("[socket] complaint:join failed:", errText(error));
  }
}

async function handleComplaintSend(socket, payload) {
  const user = socket.data.user;
  try {
    const complaintId =
      payload && typeof payload === "object" ? payload.complaintId : undefined;
    const content = readContent(payload);
    if (content === null) {
      socket.emit("chat:error", { message: "Message cannot be empty." });
      return;
    }
    if (content.length > MAX_THREAD_LENGTH) {
      socket.emit("chat:error", {
        message: `Message must be ${MAX_THREAD_LENGTH} characters or fewer.`,
      });
      return;
    }

    // Validation runs first so a rejected message never costs the sender part
    // of their allowance.
    const settings = await getSettings();
    const verdict = checkRateLimit(user.id, settings.chatRateLimitPerMin);
    if (!verdict.ok) {
      socket.emit("chat:error", {
        message: `You are sending messages too quickly. Try again in ${verdict.retryInSeconds}s.`,
      });
      return;
    }

    const complaint = await complaintForUser(user, complaintId);
    if (!complaint) {
      socket.emit("chat:error", { message: "Conversation not available." });
      return;
    }

    const row = await prisma.complaintMessage.create({
      data: {
        complaintId: complaint.id,
        senderId: user.id,
        senderRole: user.role,
        content,
      },
      select: COMPLAINT_MESSAGE_FIELDS,
    });
    await prisma.complaint.update({
      where: { id: complaint.id },
      data: { updatedAt: new Date() },
    });

    // The sender is in the room too and merges by id, so no separate echo.
    io.to(`complaint:${complaint.id}`).emit("complaint:new", toComplaintMessage(row));
  } catch (error) {
    console.error("[socket] complaint:send failed:", errText(error));
    socket.emit("chat:error", {
      message: "Message could not be delivered. Please try again.",
    });
  }
}

/* ------------------------------------------------------ live chat events */

const LIVE_MESSAGE_FIELDS = {
  id: true,
  conversationId: true,
  senderRole: true,
  content: true,
  createdAt: true,
};

function toLiveMessage(row) {
  return {
    id: row.id,
    conversationId: row.conversationId,
    senderRole: row.senderRole,
    content: row.content,
    createdAt: row.createdAt.toISOString(),
  };
}

/**
 * Picks a pseudonym no other conversation is using.
 *
 * A duplicate is not merely cosmetic: the admin inbox lists conversations by
 * pseudonym, so two students both showing "Anonymous #42" makes it ambiguous
 * which thread an admin is answering. The REST twin in /api/livechat picks
 * unchecked too, so a collision can come from either side — the check here
 * narrows, not eliminates, the window (the twin needs the same fix).
 *
 * Ten attempts against 990 slots covers all but ~1% of collision odds even in
 * a busy room; past that, accepting a duplicate beats refusing the student a
 * conversation.
 */
async function pickLivePseudonym() {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    const candidate = `Anonymous #${10 + Math.floor(Math.random() * 990)}`;
    const clash = await prisma.liveConversation.findFirst({
      where: { pseudonym: candidate },
      select: { id: true },
    });
    if (!clash) return candidate;
  }
  return `Anonymous #${10 + Math.floor(Math.random() * 990)}`;
}

/** Creates the student's conversation if this is their first touch — the
 *  socket twin of ensureConversation in /api/livechat.
 *
 *  The find-then-create shape had a race: two concurrent first-touches
 *  (two tabs opened simultaneously) both saw "no existing", both created,
 *  and the second hit a P2002 on the unique userId — which surfaced to the
 *  user as "Message could not be delivered." The P2002 catch below turns
 *  that into a re-read: the other tab's create won, so read it back. */
async function ensureLiveConversation(userId) {
  const existing = await prisma.liveConversation.findUnique({
    where: { userId },
    select: { id: true, pseudonym: true, status: true },
  });
  if (existing) return existing;
  try {
    return await prisma.liveConversation.create({
      data: { userId, pseudonym: await pickLivePseudonym() },
      select: { id: true, pseudonym: true, status: true },
    });
  } catch (error) {
    // P2002 = unique constraint violation — the conversation was created
    // between our find and our create. The row exists now; read it.
    if (error?.code === "P2002") {
      return prisma.liveConversation.findUnique({
        where: { userId },
        select: { id: true, pseudonym: true, status: true },
      });
    }
    throw error;
  }
}

async function handleLivechatJoin(socket, payload) {
  const user = socket.data.user;
  try {
    if (user.role === "ADMIN") {
      // An admin joins a specific conversation's room when they open it.
      const id =
        payload && typeof payload === "object" ? payload.conversationId : undefined;
      if (typeof id !== "string" || id.trim().length === 0) return;
      const conversation = await prisma.liveConversation.findUnique({
        where: { id },
        select: { id: true },
      });
      if (conversation) await socket.join(`live:${conversation.id}`);
      return;
    }

    // A student joins their own conversation — resolved BY their user id, so
    // a crafted payload has no conversation to point at.
    const conversation = await ensureLiveConversation(user.id);
    await socket.join(`live:${conversation.id}`);
    socket.emit("livechat:conversation", {
      id: conversation.id,
      pseudonym: conversation.pseudonym,
      status: conversation.status,
    });
  } catch (error) {
    console.error("[socket] livechat:join failed:", errText(error));
  }
}

async function handleLivechatSend(socket, payload) {
  const user = socket.data.user;
  try {
    // livechat:send is the student's voice by definition — the conversation is
    // resolved BY the sender's user id and the row is written with
    // senderRole "STUDENT". Without this check an admin emitting the event
    // would create a conversation owned by the admin carrying a student-voiced
    // message, which is indistinguishable from a student's own words in the
    // inbox. Admins reply through livechat:reply.
    if (user.role !== "STUDENT") {
      socket.emit("chat:error", { message: "Conversation not available." });
      return;
    }

    const content = readContent(payload);
    if (content === null) {
      socket.emit("chat:error", { message: "Message cannot be empty." });
      return;
    }
    if (content.length > MAX_THREAD_LENGTH) {
      socket.emit("chat:error", {
        message: `Message must be ${MAX_THREAD_LENGTH} characters or fewer.`,
      });
      return;
    }

    // Validation runs first so a rejected message never costs the sender part
    // of their allowance.
    const settings = await getSettings();
    const verdict = checkRateLimit(user.id, settings.chatRateLimitPerMin);
    if (!verdict.ok) {
      socket.emit("chat:error", {
        message: `You are sending messages too quickly. Try again in ${verdict.retryInSeconds}s.`,
      });
      return;
    }

    const conversation = await ensureLiveConversation(user.id);
    if (conversation.status === "CLOSED") {
      await prisma.liveConversation.update({
        where: { id: conversation.id },
        data: { status: "OPEN" },
      });
    }

    const row = await prisma.liveMessage.create({
      data: {
        conversationId: conversation.id,
        senderId: user.id,
        senderRole: "STUDENT",
        content,
      },
      select: LIVE_MESSAGE_FIELDS,
    });
    await prisma.liveConversation.update({
      where: { id: conversation.id },
      data: { adminUnread: { increment: 1 }, updatedAt: new Date() },
    });

    const message = toLiveMessage(row);
    io.to(`live:${conversation.id}`).emit("livechat:new", message);
    // The inbox bump lets every connected admin's badge move without a
    // refetch; admins not in the room still catch up over REST on open.
    io.to("admins").emit("livechat:inbox", {
      conversationId: conversation.id,
      lastMessage: message,
    });
  } catch (error) {
    console.error("[socket] livechat:send failed:", errText(error));
    socket.emit("chat:error", {
      message: "Message could not be delivered. Please try again.",
    });
  }
}

async function handleLivechatReply(socket, payload) {
  const user = socket.data.user;
  try {
    const conversationId =
      payload && typeof payload === "object" ? payload.conversationId : undefined;
    const content = readContent(payload);
    if (content === null) {
      socket.emit("chat:error", { message: "Message cannot be empty." });
      return;
    }
    if (content.length > MAX_THREAD_LENGTH) {
      socket.emit("chat:error", {
        message: `Message must be ${MAX_THREAD_LENGTH} characters or fewer.`,
      });
      return;
    }
    if (user.role !== "ADMIN" || typeof conversationId !== "string") {
      socket.emit("chat:error", { message: "Conversation not available." });
      return;
    }

    // Validation runs first so a rejected message never costs the sender part
    // of their allowance.
    const settings = await getSettings();
    const verdict = checkRateLimit(user.id, settings.chatRateLimitPerMin);
    if (!verdict.ok) {
      socket.emit("chat:error", {
        message: `You are sending messages too quickly. Try again in ${verdict.retryInSeconds}s.`,
      });
      return;
    }

    const conversation = await prisma.liveConversation.findUnique({
      where: { id: conversationId },
      // The student rides along because the reply notifies them, and the email
      // needs a name and an address to address them by.
      select: {
        id: true,
        user: { select: { id: true, name: true, email: true } },
      },
    });
    if (!conversation) {
      socket.emit("chat:error", { message: "Conversation not available." });
      return;
    }

    const row = await prisma.liveMessage.create({
      data: {
        conversationId: conversation.id,
        senderId: user.id,
        senderRole: "ADMIN",
        content,
      },
      select: LIVE_MESSAGE_FIELDS,
    });
    await prisma.liveConversation.update({
      where: { id: conversation.id },
      data: { userUnread: { increment: 1 }, updatedAt: new Date() },
    });

    io.to(`live:${conversation.id}`).emit("livechat:new", toLiveMessage(row));

    // Tell the student — bell, Web Push and email. The wording and the rule
    // live in lib/notify-student.mjs, shared with the REST twin of this write
    // path (pages/api/admin/livechat/[id].ts) so the two cannot drift; only
    // the transports are supplied here. Awaited, not fired and forgotten: this
    // process stays alive, but a silent failure would leave the admin thinking
    // the student had been told. The notifier never throws.
    await notifyNewAdminMessage({
      prisma,
      userId: conversation.user.id,
      name: conversation.user.name,
      email: conversation.user.email,
      push: (recipientId, title, body) =>
        pushToRecipients([recipientId], title, body),
      sendEmail,
    });
  } catch (error) {
    console.error("[socket] livechat:reply failed:", errText(error));
    socket.emit("chat:error", {
      message: "Message could not be delivered. Please try again.",
    });
  }
}

/* ------------------------------------------------------- message deletion */

/**
 * Soft-deletes a message and broadcasts the removal. Authorization per table:
 * a student may delete their OWN messages only; an admin may delete any
 * (moderation). Ownership is checked in the UPDATE's WHERE — a student
 * targeting someone else's row updates zero rows and gets the same 404-shaped
 * refusal, with no existence oracle.
 *
 * Payload: { messageId } (the message's own id — NOT the conversation/complaint
 * id, so each handler routes by the row it finds).
 */
async function deleteMessage(socket, payload, table, roomPrefix, event) {
  const user = socket.data.user;
  try {
    const id =
      payload && typeof payload === "object" ? payload.messageId : undefined;
    if (typeof id !== "string" || id.trim().length === 0) {
      socket.emit("chat:error", { message: "Message not available." });
      return;
    }

    // Students only delete their own; admins anything. updateMany (not update)
    // because the where is a filter, not a unique key.
    const where = {
      id: id.trim(),
      deletedAt: null,
      ...(user.role === "STUDENT" ? { senderId: user.id } : {}),
    };

    const rows = await prisma[table].updateMany({
      where,
      data: { deletedAt: new Date() },
    });
    if (rows.count === 0) {
      // Already deleted, never existed, or not theirs — all the same answer.
      socket.emit("chat:error", { message: "Message not available." });
      return;
    }

    // The room key differs per table (conversationId vs complaintId); fetch
    // the row's parent so the removal reaches the right room. senderRole and
    // readAt ride along on the liveMessage shape to feed the unread-counter
    // repair below.
    const row = await prisma[table].findUnique({
      where: { id: id.trim() },
      select:
        table === "liveMessage"
          ? { conversationId: true, senderRole: true, readAt: true }
          : { complaintId: true },
    });
    if (!row) return;
    const parentId = row.conversationId ?? row.complaintId;

    // Same unread-counter repair as the REST twin (DELETE
    // /api/livechat/messages/[id]): the inbox badges are denormalized counts,
    // so soft-deleting a message nobody had read yet must take its count
    // back out, or the badge stays lit forever pointing at a bubble no view
    // will ever render again. The `gt: 0` guard floors the decrement — a
    // drifted counter must not be pushed negative. Complaint threads have no
    // such counter (their unread dot is an aggregate already filtered by
    // deletedAt), so this is liveMessage-only.
    if (table === "liveMessage" && row.readAt === null) {
      await prisma.liveConversation.updateMany({
        where:
          row.senderRole === "STUDENT"
            ? { id: row.conversationId, adminUnread: { gt: 0 } }
            : { id: row.conversationId, userUnread: { gt: 0 } },
        data:
          row.senderRole === "STUDENT"
            ? { adminUnread: { decrement: 1 } }
            : { userUnread: { decrement: 1 } },
      });
    }

    io.to(`${roomPrefix}:${parentId}`).emit(event, { id: id.trim(), parentId });
  } catch (error) {
    console.error(`[socket] ${event} failed:`, errText(error));
    socket.emit("chat:error", {
      message: "Message could not be removed. Please try again.",
    });
  }
}

function handleLivechatDelete(socket, payload) {
  return deleteMessage(socket, payload, "liveMessage", "live", "livechat:deleted");
}

function handleComplaintDelete(socket, payload) {
  return deleteMessage(
    socket,
    payload,
    "complaintMessage",
    "complaint",
    "complaint:deleted",
  );
}

/* ------------------------------------------------------- notification events */

/**
 * The admin's push-notification channel: notification:send { title, body,
 * userId? }. Broadcasts fan out to every active student as one Notification
 * row each; a userId targets one student (resolved from the lookup feature's
 * user ids). Every recipient gets: a row (the in-app center), a realtime
 * notification:new to their personal room, and a Web Push per subscription.
 */
async function handleNotificationSend(socket, payload) {
  const user = socket.data.user;
  try {
    if (user.role !== "ADMIN") {
      socket.emit("chat:error", { message: "Not authorized." });
      return;
    }

    const title =
      payload && typeof payload === "object"
        ? typeof payload.title === "string"
          ? payload.title.trim()
          : ""
        : "";
    const body =
      payload && typeof payload === "object"
        ? typeof payload.body === "string"
          ? payload.body.trim()
          : ""
        : "";
    if (!title || !body) {
      socket.emit("chat:error", {
        message: "A notification needs a title and a message.",
      });
      return;
    }
    // No length caps on persistence or realtime — same policy as the REST
    // twin (pages/api/admin/notifications.ts): the admin is the trusted
    // author, the Postgres TEXT column is unlimited, and only non-empty is
    // checked. The Web Push payload is the one exception: pushToRecipients
    // truncates it to the protocol's ~4KB ceiling (see below), because the
    // push service would otherwise reject the send with a 413 that nothing
    // here would surface.

    // Rate-limit by the ADMIN's id: broadcast fan-out is expensive (a row +
    // push per student), so an admin's runaway script must not hammer it.
    const settings = await getSettings();
    const verdict = checkRateLimit(user.id, settings.chatRateLimitPerMin);
    if (!verdict.ok) {
      socket.emit("chat:error", {
        message: `Sending too quickly. Try again in ${verdict.retryInSeconds}s.`,
      });
      return;
    }

    // Recipients: one student, or every active student.
    const recipients = payload.userId
      ? await prisma.user.findMany({
          where: { id: payload.userId, role: "STUDENT", isActive: true },
          select: { id: true },
        })
      : await prisma.user.findMany({
          where: { role: "STUDENT", isActive: true },
          select: { id: true },
        });
    if (recipients.length === 0) {
      socket.emit("chat:error", { message: "No recipients found." });
      return;
    }

    // Per-recipient create() rather than one createMany: the row id must
    // ride on the notification:new payload. The overlay dedupes its realtime
    // entries against its mount-time catch-up fetch, and without a real id
    // it used to fall back to title+body matching — so the same
    // notification could display twice, or a repeat announcement (same
    // title, same text) be wrongly skipped. At this scale (hundreds of
    // recipients) the extra round-trips over createMany are fine, and the
    // creates are issued in parallel so the wall time stays one round-trip.
    const rows = await Promise.all(
      recipients.map((r) =>
        prisma.notification.create({ data: { userId: r.id, title, body } }),
      ),
    );
    for (const row of rows) {
      io.to(`user:${row.userId}`).emit("notification:new", {
        id: row.id,
        title,
        body,
        // JSON payloads must be serializable; Date objects are not.
        createdAt: row.createdAt.toISOString(),
      });
    }

    // Acknowledge as soon as the rows + realtime fan-out are done — that is
    // what the admin's UI actually waits on. The Web Push loop is detached
    // (this is a long-lived Render process, not a serverless function, so
    // there is no freeze-the-invocation hazard like the REST twin's; the
    // detach only keeps a slow push service from delaying notification:sent)
    // and its errors are logged, never propagated to the admin.
    socket.emit("notification:sent", { count: rows.length });

    const recipientIds = rows.map((r) => r.userId);
    void pushToRecipients(recipientIds, title, body).catch((error) => {
      console.error("[socket] notification push fan-out failed:", errText(error));
    });
  } catch (error) {
    console.error("[socket] notification:send failed:", errText(error));
    socket.emit("chat:error", {
      message: "Notification could not be sent. Please try again.",
    });
  }
}

/**
 * Web Push delivery, batched across every recipient at once (one
 * subscription query, not one per student — the same shape as the REST
 * twin's pushToRecipients in pages/api/admin/notifications.ts). Best-effort
 * by design: a failing subscription (410 Gone is the standard "uninstalled"
 * signal) is deleted rather than retried, and an error pushing to one
 * device must never block the loop or the realtime emit. Silently a no-op
 * when VAPID keys are absent (the channel degrades to in-app only — the
 * Notification rows and realtime events are the source of truth).
 *
 * The payload is truncated to PUSH_TITLE_MAX/PUSH_BODY_MAX characters: the
 * Web Push protocol has a ~4KB total payload ceiling and answers anything
 * above it with a 413 that this loop would otherwise swallow invisibly.
 * The full text always persists in the database and the realtime event —
 * only the lock-screen preview is clipped. Values mirror the REST twin
 * exactly so a student sees the same clipping whichever path sent it.
 */
const PUSH_TITLE_MAX = 100;
const PUSH_BODY_MAX = 3000;

async function pushToRecipients(userIds, title, body) {
  if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY) return;
  try {
    // web-push is imported lazily: it is optional infrastructure, and a
    // missing/failed import must not take the socket server down.
    const webpush = (await import("web-push")).default;
    webpush.setVapidDetails(
      process.env.VAPID_SUBJECT || "mailto:studentaffairs@unilorin.edu.ng",
      process.env.VAPID_PUBLIC_KEY,
      process.env.VAPID_PRIVATE_KEY,
    );

    const subs = await prisma.pushSubscription.findMany({
      where: { userId: { in: userIds } },
    });
    for (const sub of subs) {
      try {
        await webpush.sendNotification(
          {
            endpoint: sub.endpoint,
            keys: { p256dh: sub.p256dh, auth: sub.auth },
          },
          JSON.stringify({
            title: title.slice(0, PUSH_TITLE_MAX),
            body: body.slice(0, PUSH_BODY_MAX),
          }),
        );
      } catch (error) {
        // 404/410 = the subscription is dead (uninstalled, expired). Prune;
        // anything else is transient and the next send retries it.
        if (error?.statusCode === 404 || error?.statusCode === 410) {
          await prisma.pushSubscription.delete({ where: { id: sub.id } }).catch(() => {});
        }
      }
    }
  } catch (error) {
    console.warn("[socket] web push skipped:", errText(error));
  }
}

/**
 * Email transport for the socket runtime.
 *
 * The socket server runs under plain node and cannot import lib/email.ts, so
 * this mirrors that module's contract by hand: lazily-built Resend client,
 * the same EMAIL_FROM fallback (Resend's sandbox sender, which in production
 * delivers only to the account owner — loud in the logs, never thrown), and
 * the same [student-connect:email] prefix so a failure is recognisable
 * whichever runtime sent it.
 *
 * NEVER THROWS: every caller has already committed its side effects by the
 * time it gets here, and a failed email must not become an error on top of
 * them.
 */
let cachedResend = null;

async function sendEmail({ to, subject, html, text }) {
  if (!process.env.RESEND_API_KEY) {
    return { ok: false, error: "Email is not configured (RESEND_API_KEY)." };
  }
  try {
    if (!cachedResend) {
      const { Resend } = await import("resend");
      cachedResend = new Resend(process.env.RESEND_API_KEY);
    }
    const { error } = await cachedResend.emails.send({
      from: process.env.EMAIL_FROM ||
        "UNILORIN Student Connect <onboarding@resend.dev>",
      to,
      subject,
      html,
      text,
    });
    if (error) {
      console.error("[student-connect:email] send failed:", error.message);
      return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown email error.";
    console.error("[student-connect:email] send threw:", message);
    return { ok: false, error: message };
  }
}

/* ---------------------------------------------------------------- connection */

io.on("connection", (socket) => {
  const user = socket.data.user;
  const isAdmin = user.role === "ADMIN";

  // The pseudonym is assigned in the bootstrap IIFE at the bottom of this
  // handler, not here: a stable identity across reconnects means looking it up
  // in the database (see that IIFE for why), and this callback cannot await.
  //
  // Staff are labelled instead of anonymised — a reply from the Unit that
  // looked like "Anonymous #71" would misrepresent who was speaking — so they
  // get STAFF_LABEL; students get their LiveConversation pseudonym.

  // Every socket gets a personal room for DMs; admins share one for the inbox.
  socket.join(`user:${user.id}`);
  if (isAdmin) socket.join("admins");

  addPresence(user, socket.id);
  broadcastPresence();

  socket.on("chat:history:request", () => {
    void handleHistoryRequest(socket);
  });
  socket.on("chat:send", (payload) => {
    void handleChatSend(socket, payload);
  });
  socket.on("complaint:join", (payload) => {
    void handleComplaintJoin(socket, payload);
  });
  socket.on("complaint:send", (payload) => {
    void handleComplaintSend(socket, payload);
  });
  socket.on("livechat:join", (payload) => {
    void handleLivechatJoin(socket, payload);
  });
  socket.on("livechat:send", (payload) => {
    void handleLivechatSend(socket, payload);
  });
  socket.on("livechat:reply", (payload) => {
    void handleLivechatReply(socket, payload);
  });
  socket.on("livechat:delete", (payload) => {
    void handleLivechatDelete(socket, payload);
  });
  socket.on("complaint:delete", (payload) => {
    void handleComplaintDelete(socket, payload);
  });
  socket.on("notification:send", (payload) => {
    void handleNotificationSend(socket, payload);
  });

  // A transport-level error must not take the process with it.
  socket.on("error", (error) => {
    console.error(`[socket] socket error (${user.id}):`, errText(error));
  });

  socket.on("disconnect", () => {
    try {
      if (!isAdmin) releasePseudonym(socket.data.pseudonym);
      removePresence(user.id, socket.id);
      broadcastPresence();
    } catch (error) {
      console.error("[socket] disconnect cleanup failed:", errText(error));
    }
  });

  // Opening handshake payloads: who you are, and what has been said so far.
  void (async () => {
    try {
      // The pseudonym must be set BEFORE anything is emitted: the session
      // payload below reads it through displayNameFor. Students reuse the
      // pseudonym of their LiveConversation row — the same handle the Live
      // Chat panel shows, and already stable per student — so a reconnecting
      // student keeps the name their earlier bubbles in the global room carry.
      // A fresh random claim per connection would re-render their own recent
      // messages as someone else's after every reconnect, which is exactly
      // the confusion anonymity exists to prevent. The lookup is a read for
      // anyone who has ever touched Live Chat, and a one-time create for
      // everyone else.
      if (isAdmin) {
        socket.data.pseudonym = STAFF_LABEL;
      } else {
        try {
          socket.data.pseudonym = (await ensureLiveConversation(user.id))
            .pseudonym;
        } catch (error) {
          // The room must still work when the lookup fails (e.g. the database
          // briefly unreachable): fall back to a random in-process claim.
          // claimPseudonym/releasePseudonym exist for this path only.
          console.warn(
            "[socket] pseudonym lookup failed, falling back to random:",
            errText(error),
          );
          socket.data.pseudonym = claimPseudonym();
        }
      }

      socket.emit("session", { pseudonym: await displayNameFor(socket) });
      socket.emit("chat:history", await recentHistory());
    } catch (error) {
      console.error("[socket] connection bootstrap failed:", errText(error));
      socket.emit("chat:error", {
        message: "Could not load the chat room. Please refresh.",
      });
    }
  })();
});

/* ------------------------------------------------------------------- startup */

/** Host only — DATABASE_URL contains the password. */
function dbHost() {
  try {
    const url = new URL(process.env.DATABASE_URL);
    return `${url.hostname}:${url.port || "5432"}`;
  } catch {
    return "configured";
  }
}

httpServer.on("error", (error) => {
  if (error && error.code === "EADDRINUSE") {
    console.error(
      `  [socket] port ${PORT} is already in use. Another copy of the realtime\n` +
        "           server is probably running; stop it, or set SOCKET_PORT (and\n" +
        "           NEXT_PUBLIC_SOCKET_URL to match).",
    );
  } else {
    console.error("  [socket] server error:", errText(error));
  }
  process.exit(1);
});

httpServer.listen(PORT, () => {
  console.log("");
  console.log("  UNILORIN Student Connect — realtime server");
  console.log("  ----------------------------------------------------");
  console.log(`  socket.io    http://localhost:${PORT}`);
  console.log(`  health       http://localhost:${PORT}/health`);
  console.log(`  cors origin  ${ORIGIN}`);
  console.log(`  database     ${dbHost()}`);
  console.log(
    `  auth secret  ${
      process.env.NEXTAUTH_SECRET ? "NEXTAUTH_SECRET" : "insecure dev fallback"
    }`,
  );
  console.log("");
  console.log("  Waiting for connections. Ctrl-C to stop.");
  console.log("");
});

/* ------------------------------------------------------------------ shutdown */

let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`\n  [socket] ${signal} received, shutting down.`);
  clearInterval(pruneTimer);

  // A client stuck mid-frame must not hold the process open forever.
  const failsafe = setTimeout(() => {
    console.error("  [socket] shutdown timed out, exiting.");
    process.exit(1);
  }, 8_000);
  failsafe.unref();

  try {
    // Disconnects every client and closes the HTTP server it was attached to.
    await new Promise((done) => io.close(() => done()));
  } catch (error) {
    console.error("  [socket] error closing server:", errText(error));
  }

  try {
    await prisma.$disconnect();
  } catch (error) {
    console.error("  [socket] error disconnecting prisma:", errText(error));
  }

  console.log("  [socket] stopped cleanly.");
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

// Last resort. Every handler above is already wrapped, so reaching these means
// something escaped from a library callback. Logging and staying up is the
// right call for a chat server: dying would disconnect every student online,
// and no state here is unrecoverable.
process.on("unhandledRejection", (reason) => {
  console.error("  [socket] unhandled rejection:", errText(reason));
});
process.on("uncaughtException", (error) => {
  console.error("  [socket] uncaught exception:", errText(error));
});
