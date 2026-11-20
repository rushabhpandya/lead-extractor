"""
services/proxy_manager.py — AsyncProxyClientPool with rotating user-agents,
region-matched headers, and exponential backoff for global scraping.
"""

from __future__ import annotations

import os
import asyncio
import logging
import random
import time
from pathlib import Path
from typing import Optional, List

import httpx

logger = logging.getLogger("proxy_manager")

# ---------------------------------------------------------------------------
# User-Agent pool
# ---------------------------------------------------------------------------

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
]

# Country ISO → Accept-Language header
_LANG_HEADERS: dict[str, str] = {
    "IN": "en-IN,en-US;q=0.9,en;q=0.8,hi;q=0.7",
    "GB": "en-GB,en;q=0.9",
    "US": "en-US,en;q=0.9",
    "FR": "fr-FR,fr;q=0.9,en;q=0.8",
    "AE": "ar-AE,ar;q=0.9,en;q=0.8",
    "NL": "nl-NL,nl;q=0.9,en;q=0.8",
    "DE": "de-DE,de;q=0.9,en;q=0.8",
    "IT": "it-IT,it;q=0.9,en;q=0.8",
    "HK": "zh-HK,zh;q=0.9,en;q=0.8",
    "SG": "en-SG,en;q=0.9",
    "AU": "en-AU,en;q=0.9",
    "CA": "en-CA,en;q=0.9",
}


def _load_proxies(proxy_string: Optional[str] = None) -> List[str]:
    """
    Load proxy list from:
    1. `proxy_string` parameter (comma-separated)
    2. SCRAPER_PROXIES env var
    3. proxies.txt in project root
    """
    sources: List[str] = []

    if proxy_string:
        sources.extend([p.strip() for p in proxy_string.split(",") if p.strip()])
        return sources

    env_proxies = os.environ.get("SCRAPER_PROXIES", "")
    if env_proxies:
        sources.extend([p.strip() for p in env_proxies.split(",") if p.strip()])
        return sources

    proxy_file = Path(__file__).parent.parent / "proxies.txt"
    if proxy_file.is_file():
        lines = proxy_file.read_text(encoding="utf-8").splitlines()
        sources.extend([l.strip() for l in lines if l.strip() and not l.startswith("#")])

    return sources


def get_browser_headers(country_iso: str = "IN") -> dict:
    """Generate rotated per-request browser headers matched to target geography."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": _LANG_HEADERS.get(country_iso, "en-US,en;q=0.9"),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


class AsyncProxyClientPool:
    """
    Shared session-scoped pool of HTTP/2-enabled clients with connection limits
    (max 60 connections, max 30 keepalive) and rotating proxies.
    """

    def __init__(
        self,
        proxy_string: Optional[str] = None,
        country_iso: str = "IN",
        timeout: float = 10.0,
    ):
        self._proxies = _load_proxies(proxy_string)
        self._country_iso = country_iso
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            async with self._lock:
                if self._client is None or self._client.is_closed:
                    limits = httpx.Limits(max_connections=60, max_keepalive_connections=30)
                    proxy = random.choice(self._proxies) if self._proxies else None
                    kwargs: dict = {
                        "http2": True,
                        "limits": limits,
                        "headers": get_browser_headers(self._country_iso),
                        "timeout": self._timeout,
                        "follow_redirects": True,
                    }
                    if proxy:
                        kwargs["proxy"] = proxy
                    self._client = httpx.AsyncClient(**kwargs)
        return self._client

    async def aclose(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


def make_client(
    country_iso: str = "IN",
    timeout: float = 8.0,
    max_connections: int = 60,
    max_keepalive: int = 30,
) -> httpx.AsyncClient:
    """
    Create a shared session-scoped httpx.AsyncClient pool with HTTP/2 enabled,
    connection limits (60 max, 30 keepalive), and region-matched headers.
    Use as: `async with make_client(...) as client: ...`
    """
    limits = httpx.Limits(
        max_connections=max_connections,
        max_keepalive_connections=max_keepalive,
    )
    return httpx.AsyncClient(
        http2=True,
        limits=limits,
        headers=get_browser_headers(country_iso),
        timeout=timeout,
        follow_redirects=True,
    )
