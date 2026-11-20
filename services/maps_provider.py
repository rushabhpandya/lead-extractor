"""
services/maps_provider.py — Pluggable High-Volume Maps Places Provider.
Provides an optional, zero-friction high-volume Google Maps Places backend
via Serper.dev alongside the 100% free crawler default.

When an API key is provided (via .env SERPER_API_KEY, query param, or X-Maps-Api-Key),
queries Google Maps Places for verified real business names, addresses, star ratings,
phones, and websites, and injects them directly into the enrichment queue.

When no API key is provided, exits seamlessly with zero errors or prompts so the free
crawler runs unhindered.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urlparse

import httpx

from services.enrichment import is_cctld_mismatched

logger = logging.getLogger("maps_provider")

SERPER_MAPS_URL = "https://google.serper.dev/maps"

# Directory domains to exclude from website enrichment
MAPS_EXCLUDED_DOMAINS: Set[str] = {
    "yelp.com", "tripadvisor.com", "tripadvisor.co.uk", "facebook.com",
    "instagram.com", "yellowpages.com", "bbb.org", "foursquare.com",
    "angi.com", "thumbtack.com", "houzz.com", "apple.com", "google.com",
    "wikipedia.org", "mapquest.com", "trustpilot.com",
}


def _clean_domain(url: str) -> str:
    """Extract clean domain for duplicate checking."""
    if not url:
        return ""
    netloc = urlparse(url).netloc.lower()
    netloc = re.sub(r":\d+$", "", netloc).strip()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


async def query_serper_maps(
    query: str,
    api_key: str,
    page: int = 1,
    client: Optional[httpx.AsyncClient] = None,
) -> List[Dict[str, Any]]:
    """
    Query Serper Maps API for a given search string and page offset.
    Returns raw list of place dictionaries.
    """
    if not api_key:
        return []

    headers = {
        "X-API-KEY": api_key.strip(),
        "Content-Type": "application/json",
    }
    payload = {
        "q": query.strip(),
        "page": page,
    }

    should_close = False
    if client is None:
        client = httpx.AsyncClient(timeout=6.0)
        should_close = True

    try:
        resp = await client.post(SERPER_MAPS_URL, headers=headers, json=payload)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("places", [])
        elif resp.status_code in (401, 403):
            logger.warning("Serper Maps API key unauthorized or quota exhausted: %d", resp.status_code)
            return []
        else:
            logger.debug("Serper Maps API returned HTTP %d: %s", resp.status_code, resp.text[:200])
            return []
    except Exception as exc:
        logger.debug("Serper Maps request failed: %s", exc)
        return []
    finally:
        if should_close:
            await client.aclose()


async def harvest_maps_candidates(
    category: str,
    city: str,
    target_iso: str,
    maps_api_key: Optional[str] = None,
    districts: Optional[List[str]] = None,
    max_pages_per_query: int = 3,
) -> List[Dict[str, Any]]:
    """
    Harvest Google Maps places via Serper for the given category and location.
    Runs queries across the main city and sub-districts in parallel.
    Returns normalized candidate dictionaries ready for deep crawling & enrichment.
    """
    key = maps_api_key or os.environ.get("SERPER_API_KEY", "")
    key = key.strip() if key else ""
    if not key:
        return []

    # Prepare targeted search queries
    queries: List[str] = [f"{category} in {city}"]
    if districts:
        for d in districts[:5]:
            queries.append(f"{category} in {d}, {city}")

    candidates: List[Dict[str, Any]] = []
    seen_domains: Set[str] = set()
    seen_names: Set[str] = set()

    async with httpx.AsyncClient(timeout=7.0) as client:
        # Fan out queries across search terms and pages
        tasks = []
        for q in queries:
            for page_num in range(1, max_pages_per_query + 1):
                tasks.append(query_serper_maps(q, key, page=page_num, client=client))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if not isinstance(res, list):
                continue
            for place in res:
                if not isinstance(place, dict):
                    continue

                title = place.get("title", "").strip()
                website = place.get("website", "").strip()
                address = place.get("address", "").strip()
                phone = place.get("phoneNumber", "").strip()
                rating = place.get("rating")
                rating_count = place.get("ratingCount")

                norm_name = re.sub(r"\W+", "", title.lower())
                if not norm_name or norm_name in seen_names:
                    continue

                # Filter directory domains or invalid URLs
                dom = _clean_domain(website)
                if dom and any(dom == exc or dom.endswith("." + exc) for exc in MAPS_EXCLUDED_DOMAINS):
                    website = ""
                    dom = ""

                # Sovereign ccTLD validation on website
                if website and is_cctld_mismatched(website, target_iso):
                    continue

                if dom and dom in seen_domains:
                    continue

                seen_names.add(norm_name)
                if dom:
                    seen_domains.add(dom)

                candidates.append({
                    "name": title,
                    "title": title,
                    "url": website,
                    "website": website,
                    "address": address or f"{city}",
                    "phone": phone,
                    "rating": float(rating) if rating is not None else None,
                    "rating_count": int(rating_count) if rating_count is not None else None,
                    "snippet": f"{title} - {address}. Tel: {phone}",
                    "source": "maps_boost",
                    "root_domain": dom,
                    "category": category,
                })

    logger.info("Maps Provider harvested %d candidates for '%s in %s'", len(candidates), category, city)
    return candidates
