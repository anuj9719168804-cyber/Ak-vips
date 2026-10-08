# 🚀 VidBunker Downloader Bot

A Telegram bot (Pyrogram / Kurigram + aiohttp) that takes a **vidbunker.in watch link** and gives you the video as a **Telegram upload, stream link or direct link**, using the VidBunker worker API.

## ✨ Features

- 🔎 VidBunker link detection (`vidbunker.in/watch/<id>`, with or without `https://` / `www.`)
- 📥 Download straight to Telegram (large files are split automatically)
- 🔗 Stream link: races every candidate link, speed-tests them and opens the fastest direct CDN link (`STREAM_MODE=cdn`, default); `STREAM_MODE=proxy` relays it through the bot's own seekable `/stream` proxy
- 📥 Direct link (freshly resolved on every tap, because links expire)
- 🖼️ Thumbnail from the API, or a frame cut with FFmpeg
- 📊 Live progress, speed & ETA, cancel button
- 📦 Parallel ranged download, resumes dropped connections and verifies the exact size
- 🔁 Automatic retry with a freshly resolved link if a download fails or is too slow
- 👑 Admin tools: ban, premium, broadcast, stats, users export
- 💾 JSON storage with optional MongoDB, 🐳 Docker-ready

## 🔄 How a link is resolved (`vidbunker_api.py`)

1. `POST {"url": watch_url}` to the worker API (4 retries on 429 / 5xx / network errors, 60s per attempt) → `{"link", "filename", ...}`
2. If that fails, the worker's `GET ?url=<watch_url>` endpoint is used as a streaming fallback.
3. The real file size is probed (HEAD, then a 1-byte Range request) because the fallback has no Content-Length.

## 🛠️ Setup

```bash
cp .env.example .env      # fill in API_ID, API_HASH, BOT_TOKEN, OWNER_ID
pip install -r requirements.txt
python bot.py
```

Docker:

```bash
docker build -t vidbunker-bot .
docker run --env-file .env -p 10000:10000 vidbunker-bot
```

### Environment variables

| Variable | Meaning |
|---|---|
| `API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID` | Telegram credentials |
| `ADMINS`, `LOG_CHANNEL`, `FORCE_SUB` | Admin ids, log channel, force-join channels |
| `MONGO_URI`, `MONGO_DB_NAME` | Optional MongoDB (default DB name: `vidbunkerbot`) |
| `VIDBUNKER_API` | Worker API URL override |
| `API_RETRIES` / `API_TIMEOUT` | POST retries (default 4) / seconds per attempt (default 60) |
| `LINK_TTL` | Seconds before a stored download link is re-resolved (default 300) |
| `STREAM_MODE` | `cdn` (default, direct CDN link) or `proxy` (via `/stream`) |
| `PUBLIC_URL` | Public URL of the bot (needed for the Stream button on VPS/Docker; auto-detected on Render/Railway/Koyeb/Fly) |
| `PARALLEL_CONNECTIONS`, `MAX_FILE_SIZE_MB`, `MAX_SPLIT_MB`, `SPLIT_LARGE`, `DAILY_LIMIT` | Download tuning and limits |

## 💎 Plans, referral & user commands

- **Plans** (`/plans`): configurable with `PLANS` (price:days, `30+5` = bonus days, `lifetime`). Tap a plan → payment card → **I've Paid** notifies the admins. Each plan has its own parallel-download limit (`PARALLEL_LIMITS`). Free users: `DAILY_LIMIT` per day.
- **Referral** (`/referral`): 5 referrals → 1 day premium, 10 → +1 day.
- **Settings** (`/settings`): `/set_caption` (placeholders `{filename} {size} {quality} {source} {duration} {part} {user} {downloaded_in} {uploaded_in}`), `/see_caption`, `/del_caption`, `/set_thumb` (reply to a photo), `/view_thumb`, `/del_thumb`, `/thumb_mode`, `/setchat <chat_id|clear>` (personal dump chat).
- **File cache**: an already uploaded file is re-sent instantly (`FILE_CACHE`, optional `CACHE_CHANNEL_ID`). **Resume**: downloads cut off by a restart continue automatically (`RESUME_DOWNLOADS`).
- Everyone: `/start /help /about /plans /myplan /mystatus /referral /settings /cancel`

**Admin:** `/stats /broadcast /addpremium <id> <days|lifetime> /removepremium <id> /premiumlist /users /ban /unban /set_dump <uid> <chat|clear> /set_channel_id /channel_id /del_channel_id /clearcache`


## Diskwala fallback

If the worker API fails, `vidbunker_api.fetch_vidbunker` now tries Diskwala (`diskwala_api.py`) before the worker's GET stream:

1. **Token API** (`api2.diskwala.net/api/vidbunker/download` + `/status`) - needs `DISKWALA_TOKEN` or a Telethon `SESSION`.
2. **No-auth** - watch page / `__NEXT_DATA__` / `temp_info` scrape (always available).

See `.env.example` for the env vars. Disable with `DISKWALA_FALLBACK=0`.

## Stream fix (same as the ak-vip bot)

- Worker links (`STREAM_WORKER_HOSTS`) and links a browser would download (`Content-Disposition: attachment` or a non-media type) are always opened through the bot's `/stream` proxy, which serves them inline so they play instead of downloading. This works in `STREAM_MODE=cdn` too.
- The proxy relays with parallel ranged connections (`STREAM_PROXY_CONN`) and client disconnects (seek, pause, close) are logged at debug level, not as warnings.
- On a VPS without `PUBLIC_URL`, the server's public IP is detected at startup. The port must be open in the firewall.

## vid-player fallback

If the worker API fails, `vidbunker_api.fetch_vidbunker` tries the vid-player API next:
`GET https://vid-player.dgxserver.online/api/download?url=<watch link>`. The link is read from anywhere in the JSON reply (top level, `data`, `result`, `files[0]`, ...). Order: worker API → vid-player → Diskwala → worker GET stream. Disable with `VIDPLAYER_FALLBACK=0`.

## Same features as the ak-vip bot

- **Stream race:** the Stream button races every resolver tier (worker, vid-player, Diskwala, worker GET) plus the stored link, speed-tests them and opens the fastest one.
- **RANK_FIRST=1:** the same speed test runs before a download starts, and the fastest link is used first.
- **Convert to MP4:** MKV, TS, AVI, WEBM, MOV and similar files are remuxed to real MP4 with ffmpeg stream copy (no quality loss), so Telegram shows them as playable videos. Other files get a `.mp4` name.
- **File cache:** an already uploaded file is re-sent instantly (already in the bot, unchanged).
- **Instant cache** (`CACHE_INSTANT=1`, default): when a VidBunker link was already uploaded, the cached copy is sent straight away, before the resolver tiers are called. Turn off with `CACHE_INSTANT=0`.
