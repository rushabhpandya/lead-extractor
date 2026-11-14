"""
routers/scraper.py — Global B2B Lead Extractor API Router.
Exposes streaming extraction, execution control, and .xlsx export endpoints.
"""

from __future__ import annotations

import io
import asyncio
import json
import logging
import time
from typing import Optional

from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from services.scraper_engine import extract_leads

logger = logging.getLogger("routers.scraper")

router = APIRouter(prefix="/api", tags=["scraper"])

_stop_events: dict[str, asyncio.Event] = {}


@router.get("/extract")
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
):
    """
    Server-Sent Events extraction endpoint.
    Yields real-time events:
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
            async for lead in extract_leads(
                category=category,
                location=location,
                volume=volume,
                star=star,
                mode=mode,
            ):
                if await request.is_disconnected():
                    break
                if stop_event.is_set():
                    break

                delivered += 1
                payload = json.dumps({"type": "lead", "data": lead})
                yield f"data: {payload}\n\n"
                await asyncio.sleep(0.01)

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


@router.post("/stop/{req_id}")
async def api_stop(req_id: str):
    """Signal a running extraction to halt immediately."""
    if req_id in _stop_events:
        _stop_events[req_id].set()
        return {"ok": True}
    return {"ok": False, "reason": "unknown req_id"}


@router.post("/export/sheets")
@router.post("/export/excel")
async def api_export_excel(request: Request):
    """
    Generate an in-memory .xlsx file formatted for Google Sheets and Excel.
    Explicitly marks Phone Number cells as string data type ('s') and '@' text format
    to prevent '+...' from triggering formula evaluation errors (#ERROR!).
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
        "#", "Business Name", "Address", "Rating", "Phone Number",
        "Phone (E.164)", "Email", "Instagram Handle", "Website", "Category", "Country"
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
        cell.alignment = center_align if col_idx in (1, 4) else left_align

    # Freeze row 1
    ws.freeze_panes = "A2"

    for idx, item in enumerate(leads, start=1):
        r_val = item.get("rating")
        rating_num = float(r_val) if r_val is not None and str(r_val).strip() else ""
        phone_str = str(item.get("phone", "") or "")
        phone_e164_str = str(item.get("phone_e164", "") or phone_str)
        ig_handle = str(item.get("instagram_handle", "") or "")
        ig_display = ("@" + ig_handle) if ig_handle else ""

        row = [
            idx,
            str(item.get("name", "") or ""),
            str(item.get("address", "") or ""),
            rating_num,
            phone_str,
            phone_e164_str,
            str(item.get("email", "") or ""),
            ig_display,
            str(item.get("website", "") or ""),
            str(item.get("category", "") or ""),
            str(item.get("country_code", "") or ""),
        ]
        ws.append(row)
        current_row = idx + 1

        # Text format on phone & social cells to avoid Excel formula evaluation
        p_cell = ws.cell(row=current_row, column=5)
        p_cell.data_type = 's'
        p_cell.value = phone_str
        p_cell.number_format = '@'

        e164_cell = ws.cell(row=current_row, column=6)
        e164_cell.data_type = 's'
        e164_cell.value = phone_e164_str
        e164_cell.number_format = '@'

        ig_cell = ws.cell(row=current_row, column=8)
        ig_cell.data_type = 's'
        ig_cell.value = ig_display
        ig_cell.number_format = '@'

        # Rating formatting
        if rating_num != "":
            r_cell = ws.cell(row=current_row, column=4)
            r_cell.number_format = '0.0'
            r_cell.alignment = center_align

        ws.cell(row=current_row, column=1).alignment = center_align

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
