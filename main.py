"""
main.py — FastAPI application entry point.
Serves the static frontend, exposes real-time SSE extraction /api/extract,
and provides native Google Sheets .xlsx export via /api/export/sheets.
"""

import io
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from services.scraper_engine import extract_leads

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
logger = logging.getLogger("main")

app = FastAPI(title="B2B Lead Extractor", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global stop signals — maps request_id → asyncio.Event
_stop_events: dict[str, asyncio.Event] = {}


# ---------------------------------------------------------------------------
# SSE extraction endpoint
# ---------------------------------------------------------------------------

@app.get("/api/extract")
async def api_extract(
    request: Request,
    category: str,
    location: str,
    volume: int = 50,
    star: int = 0,
    mode: str = "approx",
    req_id: str = "",
    rating: Optional[str] = None,
    match_mode: Optional[str] = None,
    bypass_cache: bool = False,
):
    """
    Server-Sent Events stream.
    Yields real-time events:
      {"type": "status", "stage": "...", "message": "...", "scanned": N, "verified": M}
      {"type": "lead", "data": {...}}
      {"type": "done", "data": {"delivered": N, "elapsed": S}}
      {"type": "error", "data": {"message": "..."}}
    """
    if rating is not None:
        if rating.lower() in ("any", "all", "0", ""):
            star = 0
        elif rating.isdigit():
            star = int(rating)
    if match_mode:
        mode = match_mode

    stop_event = asyncio.Event()
    if req_id:
        _stop_events[req_id] = stop_event

    start_ts = time.time()
    delivered = 0

    async def event_stream():
        nonlocal delivered
        try:
            async for item in extract_leads(
                category=category,
                location=location,
                volume=volume,
                star=star,
                mode=mode,
                bypass_cache=bypass_cache,
            ):
                if await request.is_disconnected():
                    break
                if stop_event.is_set():
                    break

                if isinstance(item, dict) and item.get("type") == "status":
                    payload = json.dumps(item)
                    yield f"data: {payload}\n\n"
                    await asyncio.sleep(0)
                    continue

                delivered += 1
                payload = json.dumps({"type": "lead", "data": item})
                yield f"data: {payload}\n\n"
                await asyncio.sleep(0)

            elapsed = round(time.time() - start_ts, 1)
            done_payload = json.dumps({
                "type": "done",
                "data": {
                    "delivered": delivered,
                    "elapsed": elapsed,
                },
            })
            yield f"data: {done_payload}\n\n"

        except Exception as exc:
            logger.exception("Stream error: %s", exc)
            error_payload = json.dumps({"type": "error", "data": {"message": str(exc)}})
            yield f"data: {error_payload}\n\n"

        finally:
            if req_id and req_id in _stop_events:
                del _stop_events[req_id]

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.post("/api/stop/{req_id}")
async def api_stop(req_id: str):
    """Signal a running extraction to halt immediately."""
    if req_id in _stop_events:
        _stop_events[req_id].set()
        return {"ok": True}
    return {"ok": False, "reason": "unknown req_id"}


# ---------------------------------------------------------------------------
# Native Google Sheets / Excel .xlsx Export Endpoint
# ---------------------------------------------------------------------------

@app.post("/api/export/sheets")
@app.post("/api/export/excel")
async def api_export_sheets(request: Request):
    """
    Generate an in-memory .xlsx file formatted for Google Sheets and Excel.
    Explicitly marks Phone Number cells as string data type ('s') to prevent
    '+91...' from triggering formula evaluation errors (#ERROR!).
    """
    try:
        body = await request.json()
        if isinstance(body, dict) and "leads" in body:
            leads = body["leads"]
        elif isinstance(body, list):
            leads = body
        else:
            leads = []
    except Exception:
        leads = []

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Verified B2B Leads"

    headers = [
        "#", "Business Name", "Address", "Rating", "Lead Score",
        "Phone Number", "Phone (E.164)", "WhatsApp URL",
        "Email", "Email MX Verified", "Instagram Handle", "LinkedIn URL",
        "Website", "CMS Platform", "Meta Pixel", "Google Ads", "Category", "Country"
    ]
    ws.append(headers)

    # Styling
    dark_fill = PatternFill(start_color="121215", end_color="121215", fill_type="solid")
    header_font = Font(name="Calibri", size=11, bold=True, color="FAFAFA")
    center_align = Alignment(horizontal="center", vertical="center")
    left_align = Alignment(horizontal="left", vertical="center")

    for col_idx in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.fill = dark_fill
        cell.font = header_font
        cell.alignment = center_align if col_idx in (1, 4, 5, 10, 15, 16) else left_align

    # Freeze row 1
    ws.freeze_panes = "A2"

    for idx, item in enumerate(leads, start=1):
        r_val = item.get("rating")
        rating_num = float(r_val) if r_val is not None and str(r_val).strip() else ""
        lead_score_num = int(item.get("lead_score") or 0)
        phone_str = str(item.get("phone", "") or "")
        phone_e164_str = str(item.get("phone_e164", "") or phone_str)
        wa_url_str = str(item.get("whatsapp_url", "") or "")
        ig_handle = str(item.get("instagram_handle", "") or "")
        ig_display = ("@" + ig_handle) if ig_handle else ""
        li_url_str = str(item.get("linkedin_url", "") or "")
        mx_str = "YES" if item.get("email_mx_verified") else "NO"
        meta_str = "YES" if item.get("has_meta_pixel") else "NO"
        gads_str = "YES" if item.get("has_google_ads") else "NO"
        cms_str = str(item.get("cms_platform", "") or "Custom")

        row = [
            idx,
            str(item.get("name", "") or ""),
            str(item.get("address", "") or ""),
            rating_num,
            lead_score_num,
            phone_str,
            phone_e164_str,
            wa_url_str,
            str(item.get("email", "") or ""),
            mx_str,
            ig_display,
            li_url_str,
            str(item.get("website", "") or ""),
            cms_str,
            meta_str,
            gads_str,
            str(item.get("category", "") or ""),
            str(item.get("country_code", "") or ""),
        ]
        ws.append(row)
        current_row = idx + 1

        # Text-safe string formatting for phone numbers, WhatsApp, Instagram, LinkedIn
        p_cell = ws.cell(row=current_row, column=6)
        p_cell.data_type = 's'
        p_cell.value = phone_str
        p_cell.number_format = '@'

        e164_cell = ws.cell(row=current_row, column=7)
        e164_cell.data_type = 's'
        e164_cell.value = phone_e164_str
        e164_cell.number_format = '@'

        wa_cell = ws.cell(row=current_row, column=8)
        wa_cell.data_type = 's'
        wa_cell.value = wa_url_str
        wa_cell.number_format = '@'

        ig_cell = ws.cell(row=current_row, column=11)
        ig_cell.data_type = 's'
        ig_cell.value = ig_display
        ig_cell.number_format = '@'

        li_cell = ws.cell(row=current_row, column=12)
        li_cell.data_type = 's'
        li_cell.value = li_url_str
        li_cell.number_format = '@'

        # Score & Rating alignment
        ws.cell(row=current_row, column=1).alignment = center_align
        score_cell = ws.cell(row=current_row, column=5)
        score_cell.alignment = center_align
        score_cell.number_format = '0'

        if rating_num != "":
            r_cell = ws.cell(row=current_row, column=4)
            r_cell.number_format = '0.0'
            r_cell.alignment = center_align

        ws.cell(row=current_row, column=10).alignment = center_align
        ws.cell(row=current_row, column=15).alignment = center_align
        ws.cell(row=current_row, column=16).alignment = center_align

    # Auto-adjust column widths
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            val_str = str(cell.value or "")
            if len(val_str) > max_len:
                max_len = len(val_str)
        ws.column_dimensions[col_letter].width = min(max(max_len + 4, 12), 60)

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    return Response(
        content=out.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": 'attachment; filename="leads_export.xlsx"',
            "Access-Control-Allow-Origin": "*",
        },
    )


# ---------------------------------------------------------------------------
# Static files & frontend routes
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"

# Ensure static assets are properly mounted under /static
if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Explicit root handler serving index.html
@app.get("/", include_in_schema=False)
async def serve_root():
    if INDEX_FILE.is_file():
        return FileResponse(str(INDEX_FILE))
    return {"detail": f"Frontend index.html not found at {INDEX_FILE}"}

# Fallback for top-level root asset requests (e.g. /styles.css, /app.js)
@app.get("/{filename:path}", include_in_schema=False)
async def serve_static_root(filename: str):
    file_path = STATIC_DIR / filename
    if file_path.is_file():
        return FileResponse(str(file_path))
    return {"detail": "Not Found"}

