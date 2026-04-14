"""FastAPI server receiving Instantly webhooks and forwarding to Kommo."""

import html
import hashlib
import hmac
import logging

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


class CampaignRouteUpsertRequest(BaseModel):
    campaign_name: str = Field(min_length=1)
    pipeline_id: int
    status_id: int


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
        return JSONResponse({"status": "parse_error"}, status_code=400)

    # Filter
    if not should_process(payload):
        return JSONResponse({"status": "filtered"})

    route = route_store.get_route(payload.campaign_name)
    target_pipeline_id = route.pipeline_id if route else config.kommo_pipeline_id
    target_status_id = route.status_id if route else config.kommo_pipeline_status_id

    # Process
    result = process_webhook(
        payload=payload,
        kommo=kommo,
        store=store,
        pipeline_id=target_pipeline_id,
        status_id=target_status_id,
    )

    if result.success:
        return JSONResponse({
            "status": "ok",
            "contact_id": result.contact_id,
            "lead_id": result.lead_id,
        })

    if result.error.startswith("rate_limited"):
        return JSONResponse({"status": "retry"}, status_code=503)

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

    route_store.upsert_route(
        campaign_name=campaign_name,
        pipeline_id=payload.pipeline_id,
        status_id=payload.status_id,
    )
    return {"status": "ok"}


@app.delete("/api/admin/routes/{campaign_name}")
async def delete_campaign_route(campaign_name: str):
    route_store.delete_route(campaign_name)
    return {"status": "ok"}


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
          <th>Pipeline ID</th>
          <th>Status ID</th>
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
const routesTbody = document.getElementById("routesTbody");
const statusEl = document.getElementById("status");
const saveBtn = document.getElementById("saveBtn");

let pipelines = [];

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
  for (const st of statuses) {{
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
      <td>${{route.pipeline_id}}</td>
      <td>${{route.status_id}}</td>
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
  if (!campaign_name || !pipeline_id || !status_id) {{
    setStatus("Choose campaign, pipeline and status", true);
    return;
  }}

  const res = await fetch("/api/admin/routes", {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ campaign_name, pipeline_id, status_id }}),
  }});
  if (!res.ok) {{
    setStatus("Failed to save mapping", true);
    return;
  }}

  setStatus("Mapping saved");
  await loadRoutes();
}});

async function boot() {{
  setStatus("Loading...");
  await loadCampaigns();
  await loadPipelines();
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
