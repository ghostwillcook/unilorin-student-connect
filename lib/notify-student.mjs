/**
 * Telling a student that the Student Affairs Unit has written to them.
 *
 * WHY THIS IS A PLAIN .mjs MODULE, NOT TYPESCRIPT
 *
 * An admin reply is written on two paths, and both are live in production:
 *
 *   - the socket server  (server/socket.mjs, handleLivechatReply) — the
 *     normal path, taken whenever the socket is warm;
 *   - the REST route     (pages/api/admin/livechat/[id].ts, POST) — taken
 *     when the socket is cold.
 *
 * The admin console uses whichever is available (pages/admin/messages.tsx
 * emits livechat:reply when online and falls back to the POST when it is
 * not), so notification logic living in only one of them would fire almost
 * never. The socket server runs under plain node and cannot import
 * TypeScript, so this module is .mjs and both runtimes import it — the
 * trigger rule and the copy exist exactly once.
 *
 * What is NOT shared is the transport. Each caller injects its own `push`
 * and `sendEmail`, because each already owns a working one (the Next side
 * has lib/push.ts and lib/email.ts; the socket server has its own pushToUser
 * and no email). Everything that could drift — the rule, the wording, the
 * markup — is here.
 *
 * The one duplication accepted: the HTML shell below is a compact copy of
 * `emailShell` in lib/email-templates.ts, which is TypeScript and therefore
 * unimportable from here. If the brand palette changes, both move.
 *
 * NEVER THROWS. The caller has already committed the admin's message to the
 * database by the time this runs; a push or email failing must not turn a
 * delivered message into an error on the admin's screen. Failures are logged
 * with the [student-connect:notify] prefix so the swallow is observable.
 */

const VIOLET = "#10026F";
const PAPER = "#f7f5f0";
const INK = "#1c1a17";
const MUTED = "#6b675f";
const CARD_MAX_WIDTH = 480;

/**
 * The notification text, student-facing and deliberately short — it is a
 * lock-screen preview in a notification shade, not a paragraph.
 *
 * The message itself is never included: see the note on the email below.
 */
export const ADMIN_MESSAGE_TITLE = "New message from the Student Affairs Unit";
export const ADMIN_MESSAGE_BODY =
  "The Student Affairs Unit has sent you a private message. Sign in to read it.";

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Greeting uses the first name only — emails read friendlier that way. */
function firstName(name) {
  return String(name ?? "").trim().split(/\s+/)[0] || "there";
}

/** Table-based button: the only shape every email client renders. */
function buttonHtml(url, label) {
  return `                <table role="presentation" cellpadding="0" cellspacing="0" style="margin:24px 0;">
                  <tr>
                    <td style="background-color:${VIOLET};border-radius:8px;">
                      <a href="${escapeHtml(url)}" style="display:inline-block;padding:12px 28px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;font-size:15px;font-weight:600;color:#ffffff;text-decoration:none;">${escapeHtml(label)}</a>
                    </td>
                  </tr>
                </table>`;
}

function shell(title, bodyHtml) {
  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
  </head>
  <body style="margin:0;padding:0;background-color:${PAPER};">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:${PAPER};">
      <tr>
        <td align="center" style="padding:24px 12px;">
          <table role="presentation" width="${CARD_MAX_WIDTH}" cellpadding="0" cellspacing="0" style="width:100%;max-width:${CARD_MAX_WIDTH}px;background-color:#ffffff;border-radius:12px;overflow:hidden;">
            <tr>
              <td style="background-color:${VIOLET};padding:24px 28px;">
                <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                  <tr>
                    <td style="background-color:#ffffff;border-radius:8px;padding:3px;">
                      <img src="https://unilorinstudentconnect.com/unilorin-crest.jpg" alt="" width="38" height="38" style="display:block;width:38px;height:38px;border:0;" />
                    </td>
                    <td style="padding-left:12px;">
                      <span style="font-family:Georgia,'Times New Roman',serif;font-size:18px;font-weight:700;color:#ffffff;letter-spacing:0.02em;">UNILORIN Student Connect</span>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:28px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:${INK};font-size:15px;line-height:1.6;">
${bodyHtml}
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;
}

/**
 * "The Unit has messaged you" email.
 *
 * The message text is deliberately ABSENT — no preview, not even a snippet.
 * Student Affairs correspondence can concern a disciplinary matter or a
 * personal crisis, and an inbox is not a safe place for it: mail is indexed,
 * forwarded, synced to a family device, and previewed on a lock screen. The
 * student is told that something is waiting and where to read it; the words
 * stay behind the sign-in.
 */
export function adminMessageEmail(name, url) {
  const greeting = escapeHtml(firstName(name));
  const link = escapeHtml(url);

  const bodyHtml = `                <p style="margin:0 0 16px;">Hi ${greeting},</p>
                <p style="margin:0 0 16px;">The Student Affairs Unit has sent you a private message on UNILORIN Student Connect.</p>
${buttonHtml(url, "Read the message")}
                <p style="margin:0 0 16px;word-break:break-all;">If the button doesn't work, copy and paste this link into your browser:<br /><a href="${link}" style="color:${VIOLET};">${link}</a></p>
                <p style="margin:0;color:${MUTED};">You can reply to them from the Messages page. Please don't reply to this email — it is sent from an address that is not monitored.</p>`;

  return {
    subject: "You have a new message from the Student Affairs Unit",
    html: shell("New message from the Student Affairs Unit", bodyHtml),
    text: [
      `Hi ${firstName(name)},`,
      "",
      "The Student Affairs Unit has sent you a private message on UNILORIN Student Connect.",
      "",
      `Read the message: ${url}`,
      "",
      "You can reply to them from the Messages page. Please don't reply to this email — it is sent from an address that is not monitored.",
      "",
      "— UNILORIN Student Connect",
    ].join("\n"),
  };
}

function appBaseUrl() {
  const raw =
    process.env.APP_URL || process.env.NEXTAUTH_URL || "http://localhost:3000";
  return raw.replace(/\/+$/, "");
}

/**
 * Tells one student that an administrator has just written to them.
 *
 * THE RULE — every admin message notifies, with no batching and no cooldown.
 * A student who is messaged five times gets five emails, because the office
 * asked for it: a Student Affairs reply is a deliberate, infrequent act, and
 * a message the student never learns about is worse than a duplicate in their
 * inbox. This is a standing product decision, not an oversight — do not add a
 * throttle here without asking.
 *
 * It does mean email volume is proportional to Unit activity, so the Resend
 * plan's daily quota is a real ceiling: past it, sends fail quietly (logged,
 * never thrown — see lib/email.ts). The account is expected to be on a paid
 * plan for this reason. If the quota is ever hit, the messages themselves are
 * unaffected; only the emails are lost.
 *
 * Every step is best-effort and independently caught, so a dead push
 * subscription cannot suppress the email.
 *
 * @returns {Promise<{notified: boolean, reason?: string, errors: string[]}>}
 */
export async function notifyNewAdminMessage({
  prisma,
  userId,
  name,
  email,
  push,
  sendEmail,
}) {
  const errors = [];

  try {
    if (!prisma || !userId) {
      return { notified: false, reason: "missing-arguments", errors };
    }

    // The in-app bell. Its row is the record that the student WAS told, so a
    // failure here is logged but does not stop the push or the email.
    try {
      await prisma.notification.create({
        data: {
          userId,
          title: ADMIN_MESSAGE_TITLE,
          body: ADMIN_MESSAGE_BODY,
        },
      });
    } catch (err) {
      errors.push(`notification: ${errText(err)}`);
      console.error("[student-connect:notify] notification row failed:", errText(err));
    }

    if (typeof push === "function") {
      try {
        await push(userId, ADMIN_MESSAGE_TITLE, ADMIN_MESSAGE_BODY);
      } catch (err) {
        errors.push(`push: ${errText(err)}`);
        console.error("[student-connect:notify] push failed:", errText(err));
      }
    }

    // Only mail an address that could plausibly be one — the roster import
    // reported three unusable addresses, and a malformed one would be a
    // rejected send on every single message.
    if (typeof sendEmail === "function" && typeof email === "string" && email.includes("@")) {
      try {
        const mail = adminMessageEmail(name, `${appBaseUrl()}/student`);
        await sendEmail({
          to: email,
          subject: mail.subject,
          html: mail.html,
          text: mail.text,
        });
      } catch (err) {
        errors.push(`email: ${errText(err)}`);
        console.error("[student-connect:notify] email failed:", errText(err));
      }
    } else if (typeof sendEmail === "function") {
      errors.push("email: no usable address on file");
    }

    return { notified: true, errors };
  } catch (err) {
    // Belt and braces: the contract is that this function never throws into a
    // path that has already written the message.
    console.error("[student-connect:notify] unexpected failure:", errText(err));
    return { notified: false, reason: "unexpected", errors: [...errors, errText(err)] };
  }
}

function errText(error) {
  return error instanceof Error ? error.message : String(error);
}
