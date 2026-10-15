# Lead Extractor — Real-Time Asynchronous B2B Contact Discovery Engine

> A high-throughput, asynchronous B2B contact and star-rating discovery platform tailored for Indian local commercial ecosystems. Discovers, validates, and enriches SMB leads in real time without third-party aggregator dependencies.

![Lead Extractor UI](static/screenshot.png) <!-- Optional: save your screenshot here -->

## Key Engineering Highlights
- **Sub-Locality Query Expansion:** Overcomes the standard 20-result pagination ceiling across major Indian metropolitan hubs (Mumbai, Delhi, Bengaluru, Pune, etc.).
- **Progressive Wave SSE Streaming:** Built on FastAPI and Server-Sent Events (SSE) to deliver validated leads to the client DOM with a sub-4-second Time-to-First-Lead.
- **Strict Contact Gatekeeper:** Enforces 100% Indian phone number verification (`+91` mobile patterns and major STD landlines). Unverifiable records are rejected.
- **Deterministic & Schema-Based Star Rating Filter:** Implements dual evaluation modes (`Exact` vs. `Approx: S - 0.5 <= r <= S + 0.5`) parsed from search snippets and JSON-LD metadata.
- **Typed Export Engine:** Streams in-memory `.xlsx` spreadsheets via `openpyxl` with phone fields explicitly cast as text strings to prevent spreadsheet `#ERROR!` formula corruption.

## Tech Stack
- **Backend:** Python 3.10+, FastAPI, Uvicorn (ASGI)
- **Concurrency & Networking:** `httpx.AsyncClient`, `asyncio.Semaphore`, `asyncio.as_completed`
- **DOM & Schema Parsing:** `BeautifulSoup4` (`html.parser`), JSON-LD schema parsing
- **Data Serialization:** `openpyxl`, in-memory `io.BytesIO` streams
- **Frontend:** HTML5, CSS3 (Modern High-Contrast Dark Theme), Vanilla ES6 JavaScript (Native `EventSource` SSE)

## Quick Start

### 1. Clone & Install
```bash
git clone [https://github.com/YOUR_USERNAME/lead-extractor.git](https://github.com/YOUR_USERNAME/lead-extractor.git)
cd lead-extractor
pip install -r requirements.txt