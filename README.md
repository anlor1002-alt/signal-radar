---
title: Signal Radar
emoji: 👀
colorFrom: yellow
colorTo: pink
sdk: docker
pinned: false
---

# Signal Radar

**B2B Predictive Analytics Engine** — Detects e-commerce product trends 2-4 weeks before they peak by measuring the *acceleration* of Google search interest.

Multi-market support: Vietnam, United States, and Global.

## How It Works

Signal Radar measures **velocity** (WoW growth), **acceleration** (is growth speeding up), **consistency** (sustained above MA30), and **peak position** (how close to 30-day max).

### Classification

| Status | Condition | Meaning |
|--------|-----------|---------|
| BURSTING | WoW > 300% & interest > 20 | Explosive growth — act NOW |
| EMERGING | WoW > 100% & interest > 10 | Early signal — watch closely |
| RISING | WoW > 30% & MA7 > MA30 | Steady upward growth |
| STABLE | WoW -10% to +30% | Consistent baseline demand |
| DECLINING | WoW < -10% | Fading interest |

### Action Labels

| Label | Meaning | When |
|-------|---------|------|
| GO | Act now | Strong momentum + not at peak + high confidence |
| WATCH | Monitor | Promising but needs confirmation |
| AVOID | Skip | Declining, too late, or too weak |

### Multi-Geo Support

Track the same keyword across different markets:

| Code | Market | Language | Timezone |
|------|--------|----------|----------|
| VN | Vietnam | vi-VN | +7 |
| US | United States | en-US | -5 |
| WW | Global | en | UTC |

Same keyword in different geos = distinct tracked items.

## Commands

### Scanning & Tracking

| Command | Description |
|---------|-------------|
| `/start` | Main menu — pick a domain |
| `/scan` | Quick scan 1-5 keywords |
| `/track <kw> [VN\|US\|WW]` | Track keyword (default: VN) |
| `/untrack <kw> [VN\|US\|WW]` | Remove from tracking |
| `/mylist` | All tracked keywords with GO/WATCH/AVOID + geo flags |
| `/history <kw>` | 10-day history with sparkline + delta explanations |
| `/compare kw1, kw2, kw3` | Compare 2-5 keywords, ranked by opportunity |
| `/suggest <kw>` | Discover related trending keywords |
| `/export <kw\|project name\|all>` | Export scan history as CSV |

### Projects

Organize keywords into named campaigns:

| Command | Description |
|---------|-------------|
| `/pnew <name> [daily\|twice_daily]` | Create project |
| `/plist` | List all projects |
| `/padd <project> <kw> [VN\|US\|WW]` | Add keyword to project |
| `/pview <project>` | Project dashboard |
| `/pdel <project>` | Delete project (keywords preserved) |

Example workflow:
```
/pnew skincare twice_daily
/padd skincare "mật ong"
/padd skincare collagen US
/pview skincare
```

### Supported Domains

E-commerce, Fashion, Health & Beauty, Technology, Finance, Entertainment, Education, General (auto-detect).

## Architecture

```
signal_radar.py   — Core engine (trend fetching, velocity analysis, action labels, geo configs, suggestions)
bot.py            — Telegram bot (commands, projects, background tracker, digest)
database.py       — SQLite layer (aiosqlite) with auto-migration
run.py            — Entry point for Hugging Face Spaces
Dockerfile        — Container config (port 7860)
```

### Background Scanner

- **00:00 UTC** — Full scan of all tracked keywords (all geos)
- **12:00 UTC** — Midday scan for `twice_daily` projects only
- Per-geo processing: each market gets its own `TrendSignalConfig`
- Saves history with action labels + geo
- BURSTING/EMERGING alerts with 24h cooldown + confidence threshold
- Portfolio-style digest: GO/WATCH/AVOID sections, hottest keyword by geo, project summaries

### Alert Noise Reduction

- Only fires on status transitions to BURSTING/EMERGING
- 24-hour cooldown per keyword
- Only when action is GO or confidence >= 30

### Signal Change Explanations

Every digest and `/history` includes delta explanations:
- Confidence up/down
- Status transitions (e.g., RISING → EMERGING)
- WoW acceleration or cooling
- Interest trend direction

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your bot token
python bot.py
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Yes | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | No | Default chat ID for alerts |
| `PROXY_LIST` | No | Comma-separated proxy URLs |
| `SQLITE_DB_PATH` | No | SQLite file path (default: `signal_radar.db`) |

### Deploy to Hugging Face Spaces

```bash
git remote add space https://huggingface.co/spaces/YOUR_USER/signal-radar
git push space main
```

## Tech Stack

- Python 3.10+
- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) 22.7
- [pytrends](https://github.com/GeneralMills/pytrends) 4.9.2
- pandas, numpy
- aiosqlite
- python-dotenv
