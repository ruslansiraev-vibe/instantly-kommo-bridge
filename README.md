# Instantly-Kommo Bridge

Direct webhook-based integration between Instantly (cold email outreach) and Kommo CRM.
No third-party intermediaries (no Make.com, no Zapier).

## The problem

When using Instantly for cold outreach with Kommo as CRM, the naive approach is to forward all emails from Instantly to a corporate inbox connected to Kommo. This creates what we call the **"matryoshka problem"**:

- **Warm-up emails create junk deals** -- Instantly sends warm-up messages for sender reputation, and each one becomes a deal in Kommo
- **Negative replies flood the CRM** -- "not interested", "unsubscribe", out-of-office auto-replies all create deals that sales reps waste time closing
- **Duplicate contacts** -- no check if a contact exists, so every reply creates a new contact record
- **Duplicate deals** -- every reply from the same lead creates a new deal instead of continuing the existing one
- **Lost conversation context** -- the thread is scattered across multiple deals, so reps have no idea what was discussed

At scale (~60 new contacts/month per sending account), the CRM becomes unusable within weeks.

### Previous attempt: Make.com

A Make.com scenario (Instantly -> Make.com -> Kommo) partially solved filtering but introduced:

- Paid dependency ($29+/mo) with operation limits
- Limited debugging/customization in a visual editor
- Extra latency and another point of failure

## What this script does

A lightweight Python (FastAPI) microservice that bridges Instantly and Kommo directly:

1. **Receives webhooks** from Instantly (`reply_received`, `lead_interested`, `lead_meeting_booked`)
2. **Filters** -- only positive replies pass (Interested, Meeting Booked, Meeting Completed, Won). Warm-up, negative, OOO, auto-replies are dropped
3. **Deduplicates contacts** -- searches Kommo by email before creating. One email = one contact
4. **Deduplicates deals** -- searches for an active deal linked to the contact. One contact = one active deal
5. **Preserves context** -- every new reply is added as a note inside the existing deal
6. **Prevents duplicate processing** -- SQLite tracks processed email IDs, so webhook retries are safe

## Campaign routing UI (NEW)

You can map specific Instantly campaigns to specific Kommo pipeline/stage:

- Open admin page: `http://YOUR-SERVER:PORT/admin/routes`
- The page loads:
  - campaigns from Instantly API
  - pipelines and statuses from Kommo API
- Save mapping `campaign_name -> pipeline_id + status_id`
- If no mapping exists for a campaign, default `.env` values are used:
  - `KOMMO_PIPELINE_ID`
  - `KOMMO_PIPELINE_STATUS_ID`

## Architecture

```
Instantly (webhook)  -->  FastAPI server  -->  Kommo REST API v4
  reply_received           filter               search/create contact
  lead_interested          dedup (SQLite)        search/create lead
  lead_meeting_booked      format note           add note to lead
```

## File structure

```
config.py              -- Configuration from environment variables
dedup_store.py         -- SQLite deduplication store
kommo_client.py        -- Kommo API v4 client (contacts, leads, notes)
webhook_handler.py     -- Business logic: parse, filter, process
server.py              -- FastAPI entry point (webhook endpoint + health check)
register_webhooks.py   -- One-time script to register webhooks in Instantly
requirements.txt       -- Python dependencies
.env.example           -- Environment variables template
setup-guide.html       -- Full visual setup guide (open in browser)
```

## Quick start

```bash
# 1. Install
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install python-dotenv

# 2. Configure
cp .env.example .env
# Edit .env with your actual keys (see setup-guide.html for details)

# 3. Run
export $(cat .env | xargs)
uvicorn server:app --host 0.0.0.0 --port 8000

# 4. Verify
curl http://localhost:8000/health
# {"status":"ok"}

# 5. Register webhooks in Instantly
python register_webhooks.py https://YOUR-PUBLIC-URL/webhook/instantly

# 6. Disable old email forwarding from Instantly to Kommo inbox
```

## Prerequisites

| Service   | Requirement                   | Why                              |
|-----------|-------------------------------|----------------------------------|
| Instantly | Hyper Growth plan ($97/mo+)   | Webhooks require this plan       |
| Kommo     | Advanced / Pro / Enterprise   | API access                       |
| Python    | 3.10+                         | Runtime                          |
| Server    | Any VPS or tunnel (ngrok)     | Public URL for webhook endpoint  |

## Required credentials

| Variable                    | Where to get                                     |
|-----------------------------|--------------------------------------------------|
| `INSTANTLY_API_KEY`         | Instantly -> Settings -> Integrations -> API      |
| `INSTANTLY_WEBHOOK_SECRET`  | Generate: `openssl rand -hex 32`                  |
| `KOMMO_SUBDOMAIN`           | Your Kommo URL: **xxx**.kommo.com                 |
| `KOMMO_TOKEN`               | Kommo -> Integration -> Keys and scopes -> Long-lived token (up to 5 years, no refresh needed) |
| `KOMMO_PIPELINE_ID`         | DevTools -> Network -> `/api/v4/leads/pipelines`  |
| `KOMMO_PIPELINE_STATUS_ID`  | Same response, first stage `status_id`            |

## Instantly interest status codes

| Status            | Code | Forwarded to Kommo? |
|-------------------|------|----------------------|
| Interested        | 1    | Yes                  |
| Meeting Booked    | 2    | Yes                  |
| Meeting Completed | 3    | Yes                  |
| Won               | 4    | Yes                  |
| Not Interested    | -1   | No                   |
| Wrong Person      | -2   | No                   |
| Lost              | -3   | No                   |
| Out of Office     | 0    | No                   |
| Auto-reply        | --   | No                   |

## Full setup guide

Open `setup-guide.html` in a browser for a detailed visual guide with all steps, screenshots references, and test checklist.
