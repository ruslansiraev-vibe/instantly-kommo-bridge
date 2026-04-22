"""FastAPI server receiving Instantly webhooks and forwarding to Kommo."""

import html
import hashlib
import hmac
import logging
from collections import OrderedDict

from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, Field

from campaign_routing_store import CampaignRoutingStore
from config import Config
from instantly_client import InstantlyClient
from kommo_client import KommoClient
from dedup_store import DedupStore
from webhook_handler import (
    parse_payload,
    should_process,
    process_webhook,
)

# --- Bootstrap ---

config = Config.from_env()

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

kommo = KommoClient(subdomain=config.kommo_subdomain, token=config.kommo_token)
instantly = InstantlyClient(api_key=config.instantly_api_key)
store = DedupStore(db_path=config.db_path)
route_store = CampaignRoutingStore(db_path=config.db_path)
app = FastAPI(title="Instantly-Kommo Bridge", version="1.0.0")


class _RequestDedup:
    """In-memory LRU dedup for identical webhook deliveries from Instantly."""

    def __init__(self, max_size: int = 10_000) -> None:
        self._seen: OrderedDict[str, bool] = OrderedDict()
        self._max_size = max_size

    def is_duplicate(self, key: str) -> bool:
        if key in self._seen:
            return True
        self._seen[key] = True
        if len(self._seen) > self._max_size:
            self._seen.popitem(last=False)
        return False


_request_dedup = _RequestDedup()


class CampaignRouteUpsertRequest(BaseModel):
    campaign_name: str = Field(min_length=1)
    pipeline_id: int
    status_id: int
    task_user_id: Optional[int] = None
    task_text: Optional[str] = None


# --- Instantly event_type → human-readable lead status ---

EVENT_TO_INSTANTLY_STATUS = {
    "lead_interested": "Interested",
    "lead_meeting_booked": "Meeting Booked",
    "lead_meeting_completed": "Meeting Completed",
    "lead_won": "Won",
    "lead_out_of_office": "Out of Office",
    "lead_not_interested": "Not Interested",
    "lead_wrong_person": "Wrong Person",
    "lead_lost": "Lost",
    "lead_no_show": "No Show",
    "reply_received": "Reply",
    "email_sent": "Email Sent",
    "email_bounced": "Bounced",
    "auto_reply_received": "Auto Reply",
    "campaign_completed_for_lead_without_reply": "No Reply",
}


def _instantly_status(event_type: str) -> str:
    return EVENT_TO_INSTANTLY_STATUS.get(event_type, event_type)


# --- Logs page HTML ---

LOGS_PAGE_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Webhook Log — Instantly-Kommo Bridge</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f5f6fa; color: #1a1a2e; padding: 16px; }
    .container { max-width: 1400px; margin: 0 auto; }
    h1 { font-size: 22px; margin-bottom: 4px; }
    .subtitle { color: #666; font-size: 13px; margin-bottom: 16px; }
    .subtitle a { color: #175cd3; text-decoration: none; }

    .stats { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }
    .stat-card { background: #fff; border-radius: 8px; padding: 12px 18px;
                 border: 1px solid #e2e5ea; min-width: 110px; text-align: center; }
    .stat-card .num { font-size: 26px; font-weight: 700; }
    .stat-card .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: .5px; }
    .stat-card.processed .num { color: #067647; }
    .stat-card.filtered .num { color: #6b7280; }
    .stat-card.error .num { color: #b42318; }

    .filters { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; align-items: center; }
    .filters input, .filters select { padding: 7px 10px; border: 1px solid #d0d5dd; border-radius: 6px;
                                       font-size: 13px; background: #fff; }
    .filters input { width: 200px; }
    .filters select { min-width: 130px; }
    .btn { padding: 7px 14px; border: none; border-radius: 6px; font-size: 13px; cursor: pointer; }
    .btn-primary { background: #175cd3; color: #fff; }
    .btn-secondary { background: #fff; color: #333; border: 1px solid #d0d5dd; }
    .btn-sm { padding: 4px 8px; font-size: 12px; }
    .auto-refresh { display: flex; align-items: center; gap: 6px; margin-left: auto; font-size: 13px; color: #666; }
    .auto-refresh input { accent-color: #175cd3; }

    .table-wrap { background: #fff; border-radius: 8px; border: 1px solid #e2e5ea; overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th { background: #f9fafb; font-weight: 600; text-align: left; padding: 10px 12px;
         border-bottom: 1px solid #e2e5ea; white-space: nowrap; position: sticky; top: 0; }
    td { padding: 8px 12px; border-bottom: 1px solid #f0f1f3; vertical-align: top; }
    tr:hover td { background: #f9fafb; }

    /* Bridge status badges */
    .badge { display: inline-block; padding: 2px 8px; border-radius: 10px;
             font-size: 11px; font-weight: 600; letter-spacing: .3px; white-space: nowrap; }
    .badge-processed { background: #d1fadf; color: #067647; }
    .badge-filtered { background: #f2f4f7; color: #6b7280; }
    .badge-error { background: #fecdca; color: #b42318; }
    .badge-processing { background: #d1e9ff; color: #175cd3; }
    .badge-duplicate { background: #fef3c7; color: #92400e; }
    .badge-rate_limited { background: #fef0c7; color: #b54708; }
    .badge-parse_error { background: #fecdca; color: #b42318; }

    /* Instantly status badges — matching Instantly UI colors */
    .ist { display: inline-block; padding: 3px 10px; border-radius: 12px;
           font-size: 11px; font-weight: 600; white-space: nowrap; }
    .ist-interested      { background: #d1fadf; color: #067647; }
    .ist-meeting-booked  { background: #d1e9ff; color: #175cd3; }
    .ist-meeting-completed { background: #c7d7fe; color: #3538cd; }
    .ist-won             { background: #a6f4c5; color: #054f31; }
    .ist-out-of-office   { background: #fef0c7; color: #b54708; }
    .ist-not-interested  { background: #fecdca; color: #b42318; }
    .ist-wrong-person    { background: #fde4cf; color: #93370d; }
    .ist-lost            { background: #e4e7ec; color: #475467; }
    .ist-no-show         { background: #f2f4f7; color: #6b7280; }
    .ist-reply           { background: #e0f2fe; color: #0369a1; }
    .ist-email-sent      { background: #f2f4f7; color: #98a2b3; }
    .ist-bounced         { background: #fecdca; color: #912018; }
    .ist-auto-reply      { background: #fef3c7; color: #92400e; }
    .ist-no-reply        { background: #f2f4f7; color: #98a2b3; }
    .ist-unknown         { background: #f2f4f7; color: #667085; }

    .snippet { max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
               color: #444; font-size: 12px; }
    .snippet:hover { white-space: normal; word-break: break-word; }
    .email-link { color: #175cd3; text-decoration: none; }
    .kommo-link { color: #175cd3; text-decoration: none; font-size: 12px; }
    .ts { font-size: 12px; color: #888; white-space: nowrap; }
    .error-msg { color: #b42318; font-size: 11px; max-width: 180px; word-break: break-word; }
    .pagination { display: flex; justify-content: space-between; align-items: center;
                  padding: 10px 0; font-size: 13px; color: #666; }
    .pagination button:disabled { opacity: .4; cursor: default; }
    .loading { text-align: center; padding: 40px; color: #999; }
  </style>
</head>
<body>
<div class="container">
  <h1>Webhook Log</h1>
  <p class="subtitle">Instantly webhooks &middot; <a href="/admin/routes">Campaign routing &rarr;</a></p>

  <div class="stats" id="stats"><div class="loading">Loading stats...</div></div>

  <div class="filters">
    <input type="text" id="emailFilter" placeholder="Search by email..." />
    <select id="istStatusFilter">
      <option value="">Lead status</option>
      <option value="Interested">Interested</option>
      <option value="Meeting Booked">Meeting Booked</option>
      <option value="Meeting Completed">Meeting Completed</option>
      <option value="Won">Won</option>
      <option value="Out of Office">Out of Office</option>
      <option value="Not Interested">Not Interested</option>
      <option value="Wrong Person">Wrong Person</option>
      <option value="Lost">Lost</option>
      <option value="No Show">No Show</option>
      <option value="Reply">Reply</option>
      <option value="Email Sent">Email Sent</option>
      <option value="Bounced">Bounced</option>
      <option value="Auto Reply">Auto Reply</option>
    </select>
    <select id="statusFilter">
      <option value="">Result</option>
      <option value="processed">Processed</option>
      <option value="duplicate">Duplicate</option>
      <option value="filtered">Filtered</option>
      <option value="error">Error</option>
      <option value="rate_limited">Rate limited</option>
    </select>
    <button class="btn btn-primary" onclick="currentOffset=0; loadLogs()">Search</button>
    <button class="btn btn-secondary" onclick="resetFilters()">Reset</button>
    <div class="auto-refresh">
      <input type="checkbox" id="autoRefresh" checked />
      <label for="autoRefresh">Auto 10s</label>
    </div>
  </div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Time</th>
          <th>Lead Status</th>
          <th>Email</th>
          <th>Campaign</th>
          <th>Result</th>
          <th>Reply</th>
          <th>Kommo</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody id="tbody"><tr><td colspan="9" class="loading">Loading...</td></tr></tbody>
    </table>
  </div>

  <div class="pagination">
    <span id="pageInfo"></span>
    <div>
      <button class="btn btn-secondary btn-sm" id="prevBtn" onclick="prevPage()">&#8592; Prev</button>
      <button class="btn btn-secondary btn-sm" id="nextBtn" onclick="nextPage()">Next &#8594;</button>
    </div>
  </div>
</div>

<script>
const KOMMO = "{{KOMMO_SUBDOMAIN}}";
const PAGE_SIZE = 100;
let currentOffset = 0;
let totalRows = 0;
let refreshTimer = null;

const IST_CLASS_MAP = {
  "Interested": "ist-interested",
  "Meeting Booked": "ist-meeting-booked",
  "Meeting Completed": "ist-meeting-completed",
  "Won": "ist-won",
  "Out of Office": "ist-out-of-office",
  "Not Interested": "ist-not-interested",
  "Wrong Person": "ist-wrong-person",
  "Lost": "ist-lost",
  "No Show": "ist-no-show",
  "Reply": "ist-reply",
  "Email Sent": "ist-email-sent",
  "Bounced": "ist-bounced",
  "Auto Reply": "ist-auto-reply",
  "No Reply": "ist-no-reply",
};

function esc(v) {
  return String(v ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function istBadge(istStatus) {
  if (!istStatus) return '<span class="ist ist-unknown">—</span>';
  const cls = IST_CLASS_MAP[istStatus] || "ist-unknown";
  return `<span class="ist ${cls}">${esc(istStatus)}</span>`;
}

function bridgeBadge(status) {
  return `<span class="badge badge-${status || 'filtered'}">${esc(status)}</span>`;
}

function fmtTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  const pad = n => String(n).padStart(2, "0");
  return `${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function kommoLinks(contactId, leadId) {
  const parts = [];
  if (contactId) parts.push(`<a class="kommo-link" href="https://${KOMMO}.kommo.com/contacts/detail/${contactId}" target="_blank">C:${contactId}</a>`);
  if (leadId) parts.push(`<a class="kommo-link" href="https://${KOMMO}.kommo.com/leads/detail/${leadId}" target="_blank">L:${leadId}</a>`);
  return parts.join(" ");
}

async function loadStats() {
  try {
    const res = await fetch("/api/admin/logs/stats");
    const data = await res.json();
    const el = document.getElementById("stats");
    const order = ["processed", "duplicate", "filtered", "error", "rate_limited", "parse_error", "processing"];
    const labels = {processed:"Processed", duplicate:"Duplicates", filtered:"Filtered", error:"Errors",
                    rate_limited:"Rate limited", parse_error:"Parse errors", processing:"In progress"};
    let total = 0;
    let html = "";
    for (const [k, v] of Object.entries(data)) total += v;
    html += `<div class="stat-card"><div class="num">${total}</div><div class="label">Total</div></div>`;
    for (const key of order) {
      if (data[key]) {
        const cls = key === "processed" ? "processed" : (key === "filtered" || key === "duplicate" ? "filtered" : "error");
        html += `<div class="stat-card ${cls}"><div class="num">${data[key]}</div><div class="label">${labels[key] || key}</div></div>`;
      }
    }
    el.innerHTML = html;
  } catch(e) {
    document.getElementById("stats").innerHTML = `<div style="color:#b42318">Failed to load stats</div>`;
  }
}

async function loadLogs() {
  const email = document.getElementById("emailFilter").value.trim();
  const status = document.getElementById("statusFilter").value;
  const istStatus = document.getElementById("istStatusFilter").value;

  const params = new URLSearchParams({limit: PAGE_SIZE, offset: currentOffset});
  if (email) params.set("email", email);
  if (status) params.set("status", status);
  if (istStatus) params.set("instantly_status", istStatus);

  try {
    const res = await fetch("/api/admin/logs?" + params);
    const data = await res.json();
    totalRows = data.total || 0;
    const rows = data.rows || [];

    const tbody = document.getElementById("tbody");
    if (rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" class="loading">No events found</td></tr>';
    } else {
      tbody.innerHTML = rows.map(r => `<tr>
        <td style="color:#999">${r.id}</td>
        <td class="ts">${fmtTime(r.received_at)}</td>
        <td>${istBadge(r.instantly_status)}</td>
        <td><a class="email-link" href="mailto:${esc(r.lead_email)}">${esc(r.lead_email)}</a></td>
        <td style="font-size:12px;max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.campaign_name)}">${esc(r.campaign_name)}</td>
        <td>${bridgeBadge(r.status)}</td>
        <td class="snippet" title="${esc(r.reply_snippet)}">${esc(r.reply_snippet)}</td>
        <td>${kommoLinks(r.kommo_contact_id, r.kommo_lead_id)}</td>
        <td class="error-msg">${esc(r.error_message)}</td>
      </tr>`).join("");
    }

    document.getElementById("pageInfo").textContent =
      `Showing ${currentOffset + 1}\\u2013${Math.min(currentOffset + rows.length, totalRows)} of ${totalRows}`;
    document.getElementById("prevBtn").disabled = currentOffset === 0;
    document.getElementById("nextBtn").disabled = currentOffset + PAGE_SIZE >= totalRows;
  } catch(e) {
    document.getElementById("tbody").innerHTML = `<tr><td colspan="9" class="loading" style="color:#b42318">Failed to load: ${esc(e.message)}</td></tr>`;
  }
}

function nextPage() { currentOffset += PAGE_SIZE; loadLogs(); }
function prevPage() { currentOffset = Math.max(0, currentOffset - PAGE_SIZE); loadLogs(); }
function resetFilters() {
  document.getElementById("emailFilter").value = "";
  document.getElementById("statusFilter").value = "";
  document.getElementById("istStatusFilter").value = "";
  currentOffset = 0;
  loadLogs();
}

function startAutoRefresh() {
  stopAutoRefresh();
  refreshTimer = setInterval(() => {
    if (document.getElementById("autoRefresh").checked) {
      loadStats();
      loadLogs();
    }
  }, 10000);
}
function stopAutoRefresh() { if (refreshTimer) clearInterval(refreshTimer); }

document.getElementById("emailFilter").addEventListener("keydown", e => {
  if (e.key === "Enter") { currentOffset = 0; loadLogs(); }
});

loadStats();
loadLogs();
startAutoRefresh();
</script>
</body>
</html>
"""

# --- Webhook secret verification ---


def verify_webhook_secret(payload_bytes: bytes, header_value: str, secret: str) -> bool:
    """
    Verify Instantly webhook header.

    Instantly sends custom headers as static values (shared secret), not as HMAC signatures.
    We also accept legacy HMAC format for backward compatibility.
    """
    if hmac.compare_digest(header_value, secret):
        return True

    expected_hmac = hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected_hmac, header_value)


# --- Routes ---


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/webhook/instantly")
async def receive_webhook(request: Request):
    """
    Receive Instantly webhook, validate, filter, and forward to Kommo.
    Returns 200 quickly (Instantly expects fast response).
    """
    body = await request.body()

    # Verify webhook signature if header present
    signature = request.headers.get("X-Webhook-Secret", "")
    if signature and config.instantly_webhook_secret:
        if not verify_webhook_secret(body, signature, config.instantly_webhook_secret):
            logger.warning("Invalid webhook secret header")
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    raw = await request.json()
    logger.debug("Received webhook: %s", raw)

    # Parse
    payload = parse_payload(raw)
    if payload is None:
        evt = raw.get("event_type", "unknown")
        store.log_webhook(
            event_type=evt,
            lead_email=raw.get("lead_email", raw.get("email", "")),
            campaign_name=raw.get("campaign_name", ""),
            status="parse_error",
            instantly_status=_instantly_status(evt),
        )
        return JSONResponse({"status": "parse_error"}, status_code=400)

    # Early request-level dedup: Instantly sends each webhook 2-3 times simultaneously
    if payload.email_id:
        rdedup_key = f"{payload.event_type}:{payload.email_id}"
        if _request_dedup.is_duplicate(rdedup_key):
            logger.debug("Dropping duplicate delivery: %s for %s", payload.event_type, payload.lead_email)
            return JSONResponse({"status": "duplicate"})

    snippet = (payload.reply_text or "")[:200]
    i_status = _instantly_status(payload.event_type)

    # Filter
    if not should_process(payload):
        store.log_webhook(
            event_type=payload.event_type,
            lead_email=payload.lead_email,
            campaign_name=payload.campaign_name,
            status="filtered",
            instantly_status=i_status,
            reply_snippet=snippet,
        )
        return JSONResponse({"status": "filtered"})

    route = route_store.get_route(payload.campaign_name)
    target_pipeline_id = route.pipeline_id if route else config.kommo_pipeline_id
    target_status_id = route.status_id if route else config.kommo_pipeline_status_id
    target_task_user_id = route.task_user_id if route else None
    target_task_text = route.task_text if route else None

    # Log as "processing" before we start
    log_id = store.log_webhook(
        event_type=payload.event_type,
        lead_email=payload.lead_email,
        campaign_name=payload.campaign_name,
        status="processing",
        instantly_status=i_status,
        reply_snippet=snippet,
    )

    # Process
    result = process_webhook(
        payload=payload,
        kommo=kommo,
        store=store,
        pipeline_id=target_pipeline_id,
        status_id=target_status_id,
        task_user_id=target_task_user_id,
        task_text=target_task_text,
    )

    if result.deduplicated:
        store.update_webhook_log(log_id, status="duplicate")
        return JSONResponse({"status": "duplicate"})

    if result.success:
        store.update_webhook_log(
            log_id,
            status="processed",
            kommo_contact_id=result.contact_id or None,
            kommo_lead_id=result.lead_id or None,
        )
        return JSONResponse({
            "status": "ok",
            "contact_id": result.contact_id,
            "lead_id": result.lead_id,
        })

    if result.error.startswith("rate_limited"):
        store.update_webhook_log(log_id, status="rate_limited", error_message=result.error)
        return JSONResponse({"status": "retry"}, status_code=503)

    store.update_webhook_log(log_id, status="error", error_message=result.error)
    return JSONResponse({"status": "error", "detail": result.error}, status_code=500)


@app.get("/api/admin/routes")
async def list_campaign_routes():
    routes = route_store.list_routes()
    return {
        "default": {
            "pipeline_id": config.kommo_pipeline_id,
            "status_id": config.kommo_pipeline_status_id,
        },
        "routes": [
            {
                "campaign_name": route.campaign_name,
                "pipeline_id": route.pipeline_id,
                "status_id": route.status_id,
                "task_user_id": route.task_user_id,
                "task_text": route.task_text,
                "updated_at": route.updated_at,
            }
            for route in routes
        ],
    }


@app.post("/api/admin/routes")
async def upsert_campaign_route(payload: CampaignRouteUpsertRequest):
    campaign_name = payload.campaign_name.strip()
    if not campaign_name:
        raise HTTPException(status_code=400, detail="campaign_name cannot be empty")

    # Validate status_id: the first status in every Kommo pipeline is
    # "Incoming leads" (a system status). The API rejects it for lead creation.
    try:
        pipelines = kommo.list_pipelines()
        for p in pipelines:
            if p["id"] == payload.pipeline_id:
                statuses = p.get("statuses", [])
                if statuses and statuses[0]["id"] == payload.status_id:
                    raise HTTPException(
                        status_code=400,
                        detail=f'"{statuses[0]["name"]}" is a system status and cannot be used. '
                               f'Pick the next status (e.g. "{statuses[1]["name"]}" — {statuses[1]["id"]}).',
                    )
                break
    except HTTPException:
        raise
    except Exception:
        logger.warning("Could not validate status_id, saving as-is")

    route_store.upsert_route(
        campaign_name=campaign_name,
        pipeline_id=payload.pipeline_id,
        status_id=payload.status_id,
        task_user_id=payload.task_user_id,
        task_text=payload.task_text.strip() if payload.task_text else None,
    )
    return {"status": "ok"}


@app.delete("/api/admin/routes/{campaign_name}")
async def delete_campaign_route(campaign_name: str):
    route_store.delete_route(campaign_name)
    return {"status": "ok"}


@app.get("/api/admin/kommo/users")
async def list_kommo_users():
    try:
        return {"items": kommo.list_users()}
    except Exception as e:
        logger.exception("Failed to load Kommo users")
        return JSONResponse(
            {"items": [], "error": str(e)},
            status_code=502,
        )


@app.get("/api/admin/instantly/campaigns")
async def list_instantly_campaigns():
    try:
        return {"items": instantly.list_campaigns()}
    except Exception as e:
        logger.exception("Failed to load Instantly campaigns")
        return JSONResponse(
            {"items": [], "error": str(e)},
            status_code=502,
        )


@app.get("/api/admin/kommo/pipelines")
async def list_kommo_pipelines():
    try:
        return {"items": kommo.list_pipelines()}
    except Exception as e:
        logger.exception("Failed to load Kommo pipelines")
        return JSONResponse(
            {"items": [], "error": str(e)},
            status_code=502,
        )


@app.get("/api/admin/logs")
async def get_webhook_logs(
    limit: int = 100,
    offset: int = 0,
    status: str = "",
    email: str = "",
    event_type: str = "",
    instantly_status: str = "",
):
    return store.get_webhook_logs(
        limit=min(limit, 500),
        offset=offset,
        status_filter=status or None,
        email_filter=email or None,
        event_type_filter=event_type or None,
        instantly_status_filter=instantly_status or None,
    )


@app.get("/api/admin/logs/stats")
async def get_webhook_log_stats():
    return store.get_webhook_log_stats()


@app.get("/admin/logs", response_class=HTMLResponse)
async def webhook_logs_page():
    kommo_subdomain = html.escape(config.kommo_subdomain)
    return LOGS_PAGE_HTML.replace("{{KOMMO_SUBDOMAIN}}", kommo_subdomain)


@app.get("/admin/routes", response_class=HTMLResponse)
async def campaign_routes_admin():
    default_pipeline = html.escape(str(config.kommo_pipeline_id))
    default_status = html.escape(str(config.kommo_pipeline_status_id))
    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Instantly -> Kommo Campaign Routing</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 1080px; margin: 20px auto; padding: 0 12px; }}
    h1 {{ margin-bottom: 4px; }}
    .muted {{ color: #666; margin-top: 0; }}
    .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 14px; margin-bottom: 14px; }}
    label {{ display: block; margin: 8px 0 4px; font-size: 13px; color: #333; }}
    input, select, button {{ padding: 8px; font-size: 14px; }}
    .row {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
    th, td {{ border-bottom: 1px solid #eee; padding: 8px; text-align: left; }}
    .danger {{ background: #b42318; color: white; border: none; border-radius: 6px; }}
    .primary {{ background: #175cd3; color: white; border: none; border-radius: 6px; }}
    .status {{ margin-top: 8px; min-height: 18px; }}
  </style>
</head>
<body>
  <h1>Campaign Routing</h1>
  <p class="muted">Map Instantly campaigns to specific Kommo pipeline/status.</p>
  <p class="muted">Default fallback route: pipeline <b>{default_pipeline}</b>, status <b>{default_status}</b>.</p>

  <div class="card">
    <div class="row">
      <div style="min-width:300px; flex: 2;">
        <label for="campaignSelect">Instantly campaign</label>
        <select id="campaignSelect" style="width:100%;"></select>
      </div>
      <div style="min-width:260px; flex: 1;">
        <label for="pipelineSelect">Kommo pipeline</label>
        <select id="pipelineSelect" style="width:100%;"></select>
      </div>
      <div style="min-width:260px; flex: 1;">
        <label for="statusSelect">Kommo status</label>
        <select id="statusSelect" style="width:100%;"></select>
      </div>
      <div style="min-width:260px; flex: 1;">
        <label for="taskUserSelect">Responsible User</label>
        <select id="taskUserSelect" style="width:100%;"></select>
      </div>
      <div style="min-width:200px; flex: 1;">
        <label for="taskTextInput">Message task</label>
        <input id="taskTextInput" type="text" value="New reply check" placeholder="New reply check" style="width:100%;" />
      </div>
      <div>
        <button id="saveBtn" class="primary">Save mapping</button>
      </div>
    </div>
    <div class="status" id="status"></div>
  </div>

  <div class="card">
    <h3>Current mappings</h3>
    <table>
      <thead>
        <tr>
          <th>Campaign</th>
          <th>Pipeline</th>
          <th>Status</th>
          <th>Responsible User</th>
          <th>Message task</th>
          <th>Updated</th>
          <th></th>
        </tr>
      </thead>
      <tbody id="routesTbody"></tbody>
    </table>
  </div>

<script>
const campaignSelect = document.getElementById("campaignSelect");
const pipelineSelect = document.getElementById("pipelineSelect");
const statusSelect = document.getElementById("statusSelect");
const taskUserSelect = document.getElementById("taskUserSelect");
const taskTextInput = document.getElementById("taskTextInput");
const routesTbody = document.getElementById("routesTbody");
const statusEl = document.getElementById("status");
const saveBtn = document.getElementById("saveBtn");

let pipelines = [];
let users = [];

function setStatus(message, isError=false) {{
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#b42318" : "#067647";
}}

function escapeHtml(value) {{
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}}

function pipelineLabelById(id) {{
  const pipeline = pipelines.find(p => Number(p.id) === Number(id));
  if (!pipeline) {{
    return String(id);
  }}
  return `${{pipeline.name}} (${{pipeline.id}})`;
}}

function statusLabelByIds(pipelineId, statusId) {{
  const pipeline = pipelines.find(p => Number(p.id) === Number(pipelineId));
  if (!pipeline) {{
    return String(statusId);
  }}
  const status = (pipeline.statuses || []).find(s => Number(s.id) === Number(statusId));
  if (!status) {{
    return String(statusId);
  }}
  return `${{status.name}} (${{status.id}})`;
}}

function userNameById(id) {{
  if (!id) return "—";
  const u = users.find(x => Number(x.id) === Number(id));
  return u ? `${{u.name}} (${{u.id}})` : String(id);
}}

async function loadUsers() {{
  const res = await fetch("/api/admin/kommo/users");
  const data = await res.json();
  users = data.items || [];
  taskUserSelect.innerHTML = '<option value="">No task</option>';
  for (const u of users) {{
    const option = document.createElement("option");
    option.value = String(u.id);
    option.textContent = `${{u.name}}${{u.email ? " (" + u.email + ")" : ""}}`;
    taskUserSelect.appendChild(option);
  }}
}}

async function loadCampaigns() {{
  const res = await fetch("/api/admin/instantly/campaigns");
  const data = await res.json();
  if (!res.ok) {{
    throw new Error(data.error || "Failed to load campaigns");
  }}
  const items = (data.items || []).filter(x => x.name);
  campaignSelect.innerHTML = "";
  for (const item of items) {{
    const option = document.createElement("option");
    option.value = item.name;
    option.textContent = item.name;
    campaignSelect.appendChild(option);
  }}
}}

function fillStatuses() {{
  const selected = pipelines.find(p => String(p.id) === pipelineSelect.value);
  statusSelect.innerHTML = "";
  const statuses = selected ? (selected.statuses || []) : [];
  // Skip first status ("Incoming leads") — it's a system status that Kommo API rejects,
  // and skip closed statuses (142, 143)
  for (const st of statuses.slice(1)) {{
    if (st.id === 142 || st.id === 143) continue;
    const option = document.createElement("option");
    option.value = String(st.id);
    option.textContent = `${{st.name}} (${{st.id}})`;
    statusSelect.appendChild(option);
  }}
}}

async function loadPipelines() {{
  const res = await fetch("/api/admin/kommo/pipelines");
  const data = await res.json();
  if (!res.ok) {{
    throw new Error(data.error || "Failed to load pipelines");
  }}
  pipelines = data.items || [];
  pipelineSelect.innerHTML = "";
  for (const p of pipelines) {{
    const option = document.createElement("option");
    option.value = String(p.id);
    option.textContent = `${{p.name}} (${{p.id}})`;
    pipelineSelect.appendChild(option);
  }}
  fillStatuses();
}}

async function loadRoutes() {{
  const res = await fetch("/api/admin/routes");
  const data = await res.json();
  const routes = data.routes || [];
  routesTbody.innerHTML = "";
  for (const route of routes) {{
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${{escapeHtml(route.campaign_name)}}</td>
      <td>${{escapeHtml(pipelineLabelById(route.pipeline_id))}}</td>
      <td>${{escapeHtml(statusLabelByIds(route.pipeline_id, route.status_id))}}</td>
      <td>${{escapeHtml(userNameById(route.task_user_id))}}</td>
      <td>${{escapeHtml(route.task_text || "—")}}</td>
      <td>${{escapeHtml(route.updated_at || "")}}</td>
      <td><button class="danger" data-campaign="${{escapeHtml(route.campaign_name)}}">Delete</button></td>
    `;
    routesTbody.appendChild(tr);
  }}

  routesTbody.querySelectorAll("button[data-campaign]").forEach(btn => {{
    btn.addEventListener("click", async () => {{
      const campaign = btn.getAttribute("data-campaign");
      if (!campaign) return;
      const resDelete = await fetch(`/api/admin/routes/${{encodeURIComponent(campaign)}}`, {{
        method: "DELETE",
      }});
      if (!resDelete.ok) {{
        setStatus("Failed to delete mapping", true);
        return;
      }}
      setStatus("Mapping deleted");
      await loadRoutes();
    }});
  }});
}}

pipelineSelect.addEventListener("change", fillStatuses);

saveBtn.addEventListener("click", async () => {{
  const campaign_name = campaignSelect.value;
  const pipeline_id = Number(pipelineSelect.value);
  const status_id = Number(statusSelect.value);
  const task_user_id = taskUserSelect.value ? Number(taskUserSelect.value) : null;
  const task_text = taskTextInput.value.trim() || null;
  if (!campaign_name || !pipeline_id || !status_id) {{
    setStatus("Choose campaign, pipeline and status", true);
    return;
  }}

  const res = await fetch("/api/admin/routes", {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ campaign_name, pipeline_id, status_id, task_user_id, task_text }}),
  }});
  if (!res.ok) {{
    const err = await res.json().catch(() => ({{}}));
    setStatus(err.detail || "Failed to save mapping", true);
    return;
  }}

  setStatus("Mapping saved");
  await loadRoutes();
}});

async function boot() {{
  setStatus("Loading...");
  await Promise.all([loadCampaigns(), loadPipelines(), loadUsers()]);
  await loadRoutes();
  setStatus("Ready");
}}

boot().catch((e) => {{
  setStatus(e.message || "Failed to initialize admin page", true);
}});
</script>
</body>
</html>
"""
