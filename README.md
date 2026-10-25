# lyrics-private-bot-worker

Private Telegram song-lyrics bot as a Cloudflare Worker (JavaScript, no server, version 1.0.3-worker).

A user sends a song in a private chat, the bot first asks with inline buttons whether they want a Telegraph page or a plain text message, then finds the lyrics, translates them line by line to Persian, and delivers them. If lyrics are not found, it offers pasting the lyrics manually or sending a lyrics-page link to extract from.

## Deploy from Cloudflare (Git import, no paste needed)

1. In the Cloudflare dashboard, import this repository as a Worker.
2. Create a KV namespace and bind it to the Worker with the variable name exactly `LYRICS_KV`.
3. Add the secrets `TELEGRAM_BOT_TOKEN` and `WEBHOOK_SECRET` in the dashboard (never in this repo, never in chat).
4. Deploy, then set the Telegram webhook to `https://<worker-host>/webhook`.

Stop any other copy of this bot (for example a Termux polling bot with the same token) before testing the Worker, and never run both at once.
