# astrbot_plugin_ntfy

Subscribe to [ntfy.sh](https://ntfy.sh) (or self-hosted ntfy) topics and forward notifications to AstrBot chats.

## Features

- Real-time push via HTTP JSON stream (`/json` endpoint)
- Auto-reconnect with exponential backoff and `since=` catch-up
- Multi-topic subscription (comma-separated in a single connection)
- Basic Auth and access token authentication
- Per-session priority and tags filtering
- AI tool (`fetch_ntfy_messages`) for LLM-driven message fetching
- Session-bound routing — notifications go to the chat that subscribed

## Commands

| Command | Description |
|---|---|
| `/ntfy_sub <topic> [priority] [tags]` | Subscribe current chat to a topic |
| `/ntfy_unsub <topic>` | Unsubscribe from a topic |
| `/ntfy_list` | List subscriptions in current chat |
| `/ntfy_fetch <topic> [since]` | Fetch recent messages (e.g. `10m`, `1h`, `all`) |

## Configuration

Configure via AstrBot WebUI:

| Field | Default | Description |
|---|---|---|
| `server_url` | `https://ntfy.sh` | ntfy server URL |
| `auth_method` | `none` | `none`, `basic`, or `token` |
| `username` / `password` | — | Basic Auth credentials |
| `access_token` | — | Bearer token |
| `default_priority` | — | Default priority filter (e.g. `4,5`) |
| `default_tags` | — | Default tags filter (AND logic) |
| `reconnect_max_delay` | `60` | Max reconnection delay (seconds) |

## Requirements

- AstrBot >= v4.9.2 (KV storage API)
- `aiohttp` (auto-installed via `requirements.txt`)
