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


class AsyncProxyClientPool:
    """
    Pool of httpx.AsyncClient instances, optionally backed by rotating proxies.
    Falls back to a single non-proxy client if no proxies are configured.
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
        self._clients: List[httpx.AsyncClient] = []
        self._lock = asyncio.Lock()

    def _make_headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": _LANG_HEADERS.get(self._country_iso, "en-US,en;q=0.9"),
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    def _make_client(self, proxy: Optional[str] = None) -> httpx.AsyncClient:
        kwargs: dict = {
            "headers": self._make_headers(),
            "timeout": self._timeout,
            "follow_redirects": True,
        }
        if proxy:
            kwargs["proxy"] = proxy
        return httpx.AsyncClient(**kwargs)

    async def get(self, url: str, **kwargs) -> httpx.Response:
        """
        Perform GET with rotating proxy + exponential backoff on 429/503.
        """
        proxy = random.choice(self._proxies) if self._proxies else None
        client = self._make_client(proxy)
        backoff = 1.0

        for attempt in range(3):
            try:
                async with client:
                    resp = await client.get(url, **kwargs)
                    if resp.status_code in (429, 503):
                        wait = backoff + random.uniform(0, 0.5)
                        logger.debug("Rate limited on %s — sleeping %.1fs", url, wait)
                        await asyncio.sleep(wait)
                        backoff *= 2
                        proxy = random.choice(self._proxies) if self._proxies else None
                        client = self._make_client(proxy)
                        continue
                    return resp
            except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError) as exc:
                logger.debug("Request error on %s (attempt %d): %s", url, attempt + 1, exc)
                await asyncio.sleep(backoff)
                backoff *= 2
                proxy = random.choice(self._proxies) if self._proxies else None
                client = self._make_client(proxy)

        # Final attempt — no proxy
        async with self._make_client(None) as bare:
            return await bare.get(url, **kwargs)


def make_client(country_iso: str = "IN", timeout: float = 8.0) -> httpx.AsyncClient:
    """
    Create a single-use httpx.AsyncClient with region-matched headers.
    Use as `async with make_client(...) as client: ...`
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": _LANG_HEADERS.get(country_iso, "en-US,en;q=0.9"),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    return httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    )
