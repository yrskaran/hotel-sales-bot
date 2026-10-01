# Hotel Sales Bot

WhatsApp Cloud API + Flask + Cohere sales bot for hotel leads.

## Render settings

**Build command**

```bash
pip install -r requirements.txt
```

**Start command**

```bash
gunicorn app:app --workers 1 --threads 4 --timeout 120 --bind 0.0.0.0:$PORT
```

**Health check path**

```text
/health
```

## Required Render environment variables

```text
WHATSAPP_TOKEN=...
PHONE_NUMBER_ID=...
VERIFY_TOKEN=...
COHERE_API_KEY=...
```

## Recommended environment variables

```text
GRAPH_API_VERSION=v26.0
COHERE_MODEL=command-a-03-2025
BOT_TIMEZONE=Asia/Kolkata
MY_NOTIFY_PHONE=917500058655
BOT_WORKERS=4
MAX_HISTORY_MESSAGES=10
DEDUPE_TTL_SECONDS=86400
DEDUPE_MAX_ITEMS=5000
REQUIRE_WEBHOOK_SIGNATURE=false
```

For production, also set:

```text
META_APP_SECRET=...
REQUIRE_WEBHOOK_SIGNATURE=true
```

## Meta webhook

Callback URL:

```text
https://YOUR-RENDER-SERVICE.onrender.com/webhook
```

The Verify Token in Meta must exactly match `VERIFY_TOKEN` in Render. Subscribe the WhatsApp webhook to the `messages` field.

## Important

This test deployment intentionally uses one Gunicorn process because chat history and message-ID dedupe are currently in memory. Move state to Redis/Postgres before scaling to multiple processes or instances.

Do not commit tokens or API keys to GitHub.
