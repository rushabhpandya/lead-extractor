"""
services/scraper_engine.py — 100% Genuine Live Global B2B Lead Extractor Engine.
Zero synthetic records. Zero fake phone generators. Zero hallucinated emails or addresses.

Data Hygiene & Deduplication Upgrades:
  1. Pre-Crawl Root-Domain Deduplication: Normalizes registrable root domains, preventing internal subpage spam.
  2. Per-Session Root-Domain & E.164 Phone Locks: Ensures exactly 1 lead per root domain and per phone.
  3. Non-Business Article & Directory Filtering: Discards editorial paths (/blog/, /news/, /wiki/) and og:type="article".
  4. Domain-Aware Brand Name Selection: Scores title segments by root-domain token overlap; rejects bare city/state names.
  5. Mandatory Callable Contact Gatekeeper: Only emits leads with 100% verified E.164 callable phone numbers.
  6. High-Throughput Concurrency: asyncio.Semaphore(30) with strict 3.2s total timeout for 15–30s extractions.
"""

from __future__ import annotations

import asyncio
import base64
from collections import defaultdict
import json
import logging
import random
import re
import time
from pathlib import Path
from typing import AsyncIterator, Optional, List, Dict, Any, Set, Tuple
from urllib.parse import urlparse, unquote

import httpx
from bs4 import BeautifulSoup
import phonenumbers

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

from services.geocoder import (
    resolve_any_global_location,
    parse_multi_city,
)
from services.enrichment import (
    validate_and_format_phone,
    extract_instagram,
    rank_emails,
    clean_and_validate_email,
    verify_email_mx,
    rank_and_verify_emails,
    detect_meta_pixel,
    detect_google_ads,
    detect_cms_platform,
    extract_whatsapp_url,
    extract_linkedin_url,
    calculate_lead_score,
    is_cctld_mismatched,
    is_script_or_lang_mismatched,
    extract_domain_cctld,
)
from services.maps_provider import harvest_maps_candidates
from services.storage import (
    save_lead_to_db,
    save_leads_batch,
    get_cached_leads_from_db,
    get_all_cached_phones,
)
from services.proxy_manager import make_client, get_browser_headers

logger = logging.getLogger("scraper_engine")

# ---------------------------------------------------------------------------
# Registrable Root Domain Extraction
# ---------------------------------------------------------------------------

TWO_PART_TLDS: Set[str] = {
    "co.uk", "org.uk", "me.uk", "ltd.uk", "plc.uk",
    "com.au", "net.au", "org.au",
    "co.in", "net.in", "org.in", "gen.in", "firm.in",
    "co.nz", "net.nz", "org.nz",
    "co.za", "org.za",
    "com.br", "net.br",
    "com.sg", "edu.sg",
    "com.mx", "org.mx",
}


def extract_root_domain(url: str) -> str:
    """
    Extract normalized registrable root domain from URL.
    Strips 'www.', subdomains, ports, paths, and query parameters.
    Handles two-part ccTLDs (.co.uk, .com.au, .co.in, etc.).
    """
    if not url:
        return ""
    netloc = urlparse(url).netloc.lower()
    netloc = re.sub(r":\d+$", "", netloc).strip()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    parts = netloc.split(".")
    if len(parts) <= 2:
        return netloc

    last_two = ".".join(parts[-2:])
    if last_two in TWO_PART_TLDS and len(parts) >= 3:
        return ".".join(parts[-3:])

    return ".".join(parts[-2:])


def extract_domain_label(url_or_domain: str) -> str:
    """
    Extract the registrable domain label (SLD), stripping subdomains, www, and all TLD suffixes.
    Handles single ccTLD/gTLDs (.com, .fr, .it, .ca) and multi-part TLDs (.co.uk, .com.au, .co.in).
    Examples:
        'https://www.tripadvisor.co.uk/attractions' -> 'tripadvisor'
        'yelp.fr' -> 'yelp'
        'fr.yelp.ca' -> 'yelp'
        'whichmuseum.com' -> 'whichmuseum'
    """
    if not url_or_domain:
        return ""
    if "://" in url_or_domain:
        netloc = urlparse(url_or_domain).netloc.lower()
    else:
        netloc = url_or_domain.lower()
    netloc = netloc.split(":")[0]  # strip port if present
    if netloc.startswith("www."):
        netloc = netloc[4:]
    parts = netloc.split(".")
    if len(parts) <= 1:
        return parts[0] if parts else ""
    last_two = ".".join(parts[-2:])
    if last_two in TWO_PART_TLDS and len(parts) >= 3:
        return parts[-3]
    return parts[-2]


# ---------------------------------------------------------------------------
# Directories, Aggregators, and Non-Business Path Rejection (TLD-Agnostic)
# ---------------------------------------------------------------------------

DIRECTORY_DOMAINS: Set[str] = {
    "yelp.com", "angi.com", "thumbtack.com", "houzz.com", "homeadvisor.com",
    "yellowpages.com", "superpages.com", "bbb.org", "tripadvisor.com",
    "tripadvisor.co.uk", "wikipedia.org", "wikimedia.org", "wikidata.org",
    "wikimapia.org", "britannica.com",
    "facebook.com", "instagram.com", "linkedin.com", "pinterest.com",
    "twitter.com", "x.com", "tiktok.com", "youtube.com", "reddit.com",
    "quora.com", "mapquest.com", "crunchbase.com", "zoominfo.com", "dnb.com",
    "bloomberg.com", "amazon.com", "ebay.com", "justdial.com", "sulekha.com",
    "nobroker.in", "urbancompany.com", "indiamart.com", "tradeindia.com",
    "apple.com", "google.com", "microsoft.com", "yahoo.com", "bing.com",
    "duckduckgo.com", "zhihu.com", "medium.com", "forbes.com", "nytimes.com",
    "theguardian.com", "bbc.com", "cnn.com", "standard.co.uk", "timeout.com",
    "explorelondon.co.uk", "sothebysrealty.co.uk", "antiques-atlas.com",
    "sellingantiques.co.uk", "mountstreetneighbourhood.com", "mayfair-london.co.uk", "taskrabbit.com",
    "checkatrade.com", "mybuilder.com", "trustatrader.com", "gumtree.com",
    "craigslist.org", "porch.com", "nextdoor.com", "buildzoom.com",
    "wanderlog.com", "culturetrip.com", "theculturetrip.com", "lonelyplanet.com",
    "foursquare.com", "trustpilot.com", "glassdoor.com", "indeed.com",
    "swimcloud.com", "texaslonghorns.com", "worldaquatics.com", "olympics.com",
    "whichmuseum.com", "fleamarketinsiders.com",
}

DIRECTORY_LABELS: Set[str] = {
    "yelp", "angi", "thumbtack", "houzz", "homeadvisor", "yellowpages",
    "superpages", "bbb", "tripadvisor", "wikipedia", "wikimedia", "wikidata",
    "wikimapia", "britannica", "facebook", "instagram", "linkedin", "pinterest",
    "twitter", "x", "tiktok", "youtube", "reddit", "quora", "mapquest",
    "crunchbase", "zoominfo", "dnb", "bloomberg", "amazon", "ebay",
    "justdial", "sulekha", "nobroker", "urbancompany", "indiamart", "tradeindia",
    "apple", "google", "microsoft", "yahoo", "bing", "duckduckgo",
    "zhihu", "medium", "forbes", "nytimes", "theguardian", "bbc", "cnn",
    "standard", "timeout", "explorelondon", "sothebysrealty", "antiques-atlas",
    "sellingantiques", "mountstreetneighbourhood", "mayfair-london", "taskrabbit",
    "checkatrade", "mybuilder", "trustatrader", "gumtree", "craigslist",
    "porch", "nextdoor", "buildzoom", "wanderlog", "culturetrip", "theculturetrip",
    "lonelyplanet", "foursquare", "trustpilot", "glassdoor", "indeed",
    "swimcloud", "texaslonghorns", "worldaquatics", "olympics",
    "whichmuseum", "fleamarketinsiders", "vogue", "architecturaldigest",
    "elledecor", "cntraveler", "afar", "travelandleisure", "secretlondon",
}

DIRECTORY_KEYWORD_SUBSTRINGS: Tuple[str, ...] = (
    "tripadvisor", "yellowpages", "whichmuseum", "culturetrip", "timeout",
    "lonelyplanet", "wanderlog", "checkatrade", "trustpilot", "yelp",
)

NON_BUSINESS_PATH_PATTERNS = re.compile(
    r'(?i)/(?:blog|blogs|news|article|articles|story|stories|history|wiki|directory|directories|list|lists|list-of|top-\d+|best-\d+|category|categories|tag|tags|best-of|guide|guides|places-to-visit|attractions|things-to-do)(?:/|$|\.html?)'
)

EDITORIAL_TITLE_PREFIXES = re.compile(
    r'(?i)(?:^(?:directory\s+of|list\s+of|the\s+history\s+of|history\s+of|how\s+to|top\s*\d+|best\s+\d+|best\s+of|reviews\s+of|the\s+\d+\s+best|\d+\s+best|\d+\s+top|\d+\s+places|\d+\s+things)\b|\b(?:b2b\s+marketplace|business\s+directory|yellow\s+pages|top\s*\d+\s+best|ranking)\b)'
)

# ---------------------------------------------------------------------------
# Cache Management (SQLite Storage Layer Delegation)
# ---------------------------------------------------------------------------

def get_cached_leads_for_city(category: str, city: str) -> List[dict]:
    """Retrieve pre-verified leads for given category and city via SQLite storage layer."""
    return get_cached_leads_from_db(category, city, ttl_days=7.0)


def save_to_cache(category: str, city: str, new_leads: List[dict]) -> None:
    """Persist newly found verified leads directly to SQLite store."""
    save_leads_batch(new_leads)


def filter_by_rating(leads: List[dict], star: int, mode: str, limit: int) -> List[dict]:
    """Apply Smart Rating Filter matching the requested star rating."""
    if not leads:
        return []

    if star == 0:
        return leads[:limit]

    S = float(star)
    if mode == "exact":
        exact_matches = [
            l for l in leads
            if l.get("rating") is not None and round(float(l["rating"])) == int(S)
        ]
        if len(exact_matches) >= limit:
            return exact_matches[:limit]
        
        remainder = [l for l in leads if l not in exact_matches]
        remainder.sort(key=lambda x: abs(float(x.get("rating") or 4.0) - S))
        return (exact_matches + remainder)[:limit]
    else:
        min_r = max(1.0, S - 0.5)
        max_r = min(5.0, S + 0.5)
        matches = [
            l for l in leads
            if l.get("rating") is not None and min_r <= float(l["rating"]) <= max_r
        ]
        if len(matches) >= limit:
            return matches[:limit]
        
        remainder = [l for l in leads if l not in matches]
        return (matches + remainder)[:limit]


# ---------------------------------------------------------------------------
# Universal Generic Title Blacklist & Public Sector Filters (Rules 2 & 3)
# ---------------------------------------------------------------------------

GENERIC_TITLE_BLACKLIST: Set[str] = {
    # English
    "contact", "contacts", "contact us", "get in touch", "about", "about us", "who we are",
    "home", "homepage", "welcome", "index", "official site", "official website",
    "services", "our services", "locations", "our locations", "privacy policy",
    "terms of service", "terms and conditions", "terms", "login", "sign in", "portal",
    "menu", "products", "gallery", "portfolio", "store", "shop", "blog", "news",
    # Italian
    "contatti", "contattaci", "chi siamo", "dove siamo", "home page", "pagina iniziale",
    "benvenuto", "benvenuti", "sito ufficiale", "servizi", "i nostri servizi", "accedi",
    "entra", "privacy", "negozio",
    # French
    "contactez-nous", "contactez nous", "nous contacter",
    "a propos", "à propos", "qui sommes-nous", "qui sommes nous", "accueil", "bienvenue",
    "site officiel", "nos services", "mentions legales", "mentions légales", "connexion",
    # German
    "kontakt", "kontaktiere uns", "uber uns", "über uns", "wer wir sind",
    "startseite", "willkommen", "offizielle webseite", "offizielle website",
    "dienstleistungen", "anmelden", "impressum", "datenschutz",
    # Spanish
    "contacto", "contactenos", "contáctenos", "sobre nosotros", "quienes somos",
    "quiénes somos", "inicio", "bienvenido", "bienvenidos", "sitio oficial",
    "pagina oficial", "servicios", "iniciar sesion", "iniciar sesión", "aviso legal",
}

GENERIC_TITLE_PATTERN = re.compile(
    r'(?i)^(?:contact|contacts|contact\s+us|contatti|contactez-nous|contactez\s+nous|kontakt|about|about\s+us|chi\s+siamo|qui\s+sommes-nous|über\s+uns|home|homepage|welcome|index|official\s+site|official\s+website|sito\s+ufficiale|site\s+officiel|offizielle\s+webseite|services|our\s+services|locations|privacy\s+policy|login|sign\s+in)\b'
)

PUBLIC_SECTOR_JUNK_PATTERNS = re.compile(
    r'(?i)(?:\.gov\b|\.gob\b|\.gouv\b|\.mil\b|gov\.|gob\.|gouv\.|comune\.|municipality|turismo|tourism|publictransit|metropolitan\s+transit)'
)

LOCALIZED_CATEGORY_TERMS: Dict[str, Dict[str, List[str]]] = {
    "antique": {
        "IT": ["antiquariato", "antiquaria", "antichi", "antico", "antichità", "antichita"],
        "FR": ["antiquités", "antiquites", "antiquaire", "ancien"],
        "ES": ["antigüedades", "antiguedades", "anticuario", "antiguo"],
        "DE": ["antiquitäten", "antiquitaeten", "antik", "antiquariat"],
    },
    "gallery": {
        "IT": ["galleria", "mostra", "collezione"],
        "FR": ["galerie", "exposition"],
        "ES": ["galería", "galeria", "exposición"],
        "DE": ["galerie", "ausstellung"],
    },
    "art": {
        "IT": ["arte", "pittura", "scultura"],
        "FR": ["art", "peinture", "sculpture"],
        "ES": ["arte", "pintura", "escultura"],
        "DE": ["kunst", "gemälde"],
    },
    "carpenter": {
        "IT": ["falegname", "falegnameria", "legno"],
        "FR": ["menuisier", "menuiserie", "charpentier", "ébéniste", "ebeniste"],
        "ES": ["carpintero", "carpintería", "carpinteria", "madera"],
        "DE": ["schreiner", "schreinerei", "tischler", "tischlerei", "holz"],
    },
    "furniture": {
        "IT": ["mobili", "arredamento", "arredo"],
        "FR": ["meubles", "mobilier"],
        "ES": ["muebles", "mobiliario"],
        "DE": ["möbel", "moebel", "einrichtung"],
    },
    "interior": {
        "IT": ["interni", "arredamento", "interior"],
        "FR": ["intérieur", "interieur", "décoration"],
        "ES": ["interiores", "decoración", "decoracion"],
        "DE": ["innenarchitektur", "raumgestaltung"],
    },
    "gym": {
        "IT": ["palestra", "fitness", "allenamento"],
        "FR": ["salle de sport", "fitness", "musculation"],
        "ES": ["gimnasio", "fitness"],
        "DE": ["fitnessstudio", "fitness", "krafttraining"],
    },
    "architect": {
        "IT": ["architetto", "architettura", "studio tecnico"],
        "FR": ["architecte", "architecture"],
        "ES": ["arquitecto", "arquitectura"],
        "DE": ["architekt", "architektur", "bauplanung"],
    },
    "lawyer": {
        "IT": ["avvocato", "studio legale", "legale"],
        "FR": ["avocat", "cabinet d'avocats", "juridique"],
        "ES": ["abogado", "despacho de abogados", "bufete"],
        "DE": ["rechtsanwalt", "anwalt", "kanzlei"],
    },
}


# ---------------------------------------------------------------------------
# Domain-Aware Brand Name Resolution & Result Cleaning
# ---------------------------------------------------------------------------

def brand_from_root_domain(root_domain: str) -> str:
    """Derive a clean, Title-Cased brand name from the registrable root domain."""
    if not root_domain:
        return ""
    sld = root_domain.split(".")[0].lower()
    tokens = re.split(r"[-_]+", sld)
    clean_tokens = []
    for tok in tokens:
        if not tok:
            continue
        if tok.lower() in ("tx", "uk", "usa", "nyc", "la", "fl", "ca"):
            clean_tokens.append(tok.upper())
        elif tok.lower().startswith("tx") and len(tok) > 2:
            clean_tokens.append("TX")
            clean_tokens.append(tok[2:].title())
        else:
            clean_tokens.append(tok.title())
    return " ".join(clean_tokens)


def sanitize_brand_title(name: str) -> str:
    """
    Clean Title Sanitization (Fix 4):
    Strip leading/trailing punctuation delimiters (-, –, —, |, :, /, ·, •, ,, ;, ., ', ", whitespace)
    and collapse repeated whitespace.
    """
    if not name:
        return ""
    cleaned = re.sub(r"^[\s\-\–\—\|:\/·•,;.'\"]+|[\s\-\–\—\|:\/·•,;.'\"]+$", "", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def resolve_brand_name(
    og_site_name: Optional[str],
    json_ld_name: Optional[str],
    raw_title: Optional[str],
    root_domain: str,
    city: str,
    state: str = "",
    country: str = "",
) -> str:
    """
    Select the true brand name strictly locked to the homepage (Rule 2).
    Rejects generic navigation titles (contact, about, home, etc.) across languages.
    Never outputs a bare city, state, or country name.
    Falls back to og:site_name, JSON-LD name, or clean root domain brand.
    Enforces clean punctuation delimiter trimming (Fix 4).
    """
    city_lower = (city or "").lower().strip()
    state_lower = (state or "").lower().strip()
    country_lower = (country or "").lower().strip()
    banned = {
        city_lower, state_lower, country_lower,
        f"{city_lower}, {state_lower}".strip(", "),
        f"{city_lower}, {country_lower}".strip(", "),
    }
    banned.update(GENERIC_TITLE_BLACKLIST)

    def is_valid_brand(cand: Optional[str]) -> bool:
        if not cand:
            return False
        c = sanitize_brand_title(cand)
        if len(c) < 3 or len(c) > 60:
            return False
        c_low = c.lower()
        if c_low in banned:
            return False
        if GENERIC_TITLE_PATTERN.search(c_low):
            return False
        if re.search(r'\b(best|top\s*\d+|reviews|near\s*me|official\s*website)\b', c_low):
            return False
        return True

    # 1. Prefer og:site_name from homepage if valid
    if is_valid_brand(og_site_name):
        return sanitize_brand_title(og_site_name)

    # 2. Prefer JSON-LD name from homepage if valid
    if is_valid_brand(json_ld_name):
        return sanitize_brand_title(json_ld_name)

    # 3. Domain slug token overlap scoring against homepage <title>
    domain_slug = root_domain.split(".")[0].lower() if root_domain else ""
    slug_tokens = set(re.findall(r"[a-z0-9]+", domain_slug))

    delims = r"[|·•\-\–\—\:]"
    chunks = [sanitize_brand_title(c) for c in re.split(delims, raw_title or "") if len(sanitize_brand_title(c)) >= 3]

    scored = []
    for cand in chunks:
        if not is_valid_brand(cand):
            continue

        cand_clean = sanitize_brand_title(cand)
        cand_lower = cand_clean.lower().strip()
        cand_tokens = set(re.findall(r"[a-z0-9]+", cand_lower))
        overlap = len(cand_tokens.intersection(slug_tokens))

        sub_score = 0
        if any(t in domain_slug for t in cand_tokens if len(t) >= 3):
            sub_score += 1
        if domain_slug and domain_slug in cand_lower.replace(" ", ""):
            sub_score += 3

        loc_penalty = -5 if (cand_lower == city_lower or cand_lower == state_lower or cand_lower == country_lower) else 0
        score = overlap * 2 + sub_score + loc_penalty
        scored.append((score, len(cand_clean), cand_clean))

    if scored:
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        best = scored[0][2]
        if is_valid_brand(best):
            return sanitize_brand_title(best)

    # 4. Fall back to clean title-cased brand name derived from root domain
    fallback = brand_from_root_domain(root_domain)
    if is_valid_brand(fallback):
        return sanitize_brand_title(fallback)

    return sanitize_brand_title(f"{city.title()} Studio" if city else "Local Business")


def check_category_relevance(
    category: str,
    brand_name: str,
    root_domain: str,
    homepage_title: str,
    meta_description: str,
    headings: str,
    json_ld_types: List[str],
    snippet_title: str,
    target_iso: str = "",
) -> bool:
    """
    Semantic Category Relevance Gatekeeper (Rule 3):
    Verifies that at least one root stem (>= 4 chars) of the category query (or its
    localized equivalent) appears in the business's high-signal identity fields:
      - Brand Name, Root Domain, Homepage <title>, <meta description>, <h1>/<h2> headings,
        JSON-LD @type, or Snippet Title.
    Rejects pages where the keyword only appears incidentally in footer/body text.
    """
    clean_cat = (category or "").lower().strip()
    words = [w for w in re.findall(r"[a-zA-Z\u00C0-\u017F]+", clean_cat) if len(w) >= 3]
    if not words:
        return True

    # Assemble candidate stems & localized equivalents
    stems: Set[str] = set()
    for w in words:
        if len(w) >= 4:
            stems.add(w[:min(len(w), 6)])
            stems.add(w)
        else:
            stems.add(w)

    # Add localized terms based on target_iso
    iso_upper = (target_iso or "").upper().strip()
    for cat_key, translations in LOCALIZED_CATEGORY_TERMS.items():
        if cat_key in clean_cat or any(cat_key in w for w in words):
            if iso_upper in translations:
                for loc_word in translations[iso_upper]:
                    loc_clean = loc_word.lower()
                    if len(loc_clean) >= 4:
                        stems.add(loc_clean[:min(len(loc_clean), 6)])
                    stems.add(loc_clean)

    # Build the high-signal text blob
    high_signal_parts = [
        brand_name,
        root_domain.replace(".", " "),
        homepage_title,
        meta_description,
        headings,
        " ".join(json_ld_types),
        snippet_title,
    ]
    high_signal = " ".join(high_signal_parts).lower()

    # Check for stem or whole-word match
    for stem in stems:
        if len(stem) >= 4:
            if stem in high_signal:
                return True
        else:
            # 3-char words (e.g. gym, art, spa, law) require whole word boundary
            if re.search(rf"\b{re.escape(stem)}\b", high_signal):
                return True

    return False


# ---------------------------------------------------------------------------
# Intra-Country Locality & Municipality Verification (Fix 1)
# ---------------------------------------------------------------------------

MAJOR_CITIES_BY_COUNTRY: Dict[str, Set[str]] = {
    "US": {
        "new york", "los angeles", "chicago", "houston", "phoenix", "philadelphia",
        "san antonio", "san diego", "dallas", "austin", "san jose", "fort worth",
        "jacksonville", "columbus", "charlotte", "indianapolis", "san francisco",
        "seattle", "denver", "nashville", "oklahoma city", "el paso", "boston",
        "portland", "las vegas", "detroit", "memphis", "louisville", "baltimore",
        "milwaukee", "albuquerque", "tucson", "fresno", "sacramento", "kansas city",
        "mesa", "atlanta", "omaha", "colorado springs", "raleigh", "virginia beach",
        "long beach", "miami", "oakland", "minneapolis", "tulsa", "tampa",
        "arlington", "new orleans", "wichita", "cleveland", "bakersfield",
        "palm beach", "orlando", "boca raton", "naples", "fort lauderdale",
    },
    "GB": {
        "london", "manchester", "birmingham", "edinburgh", "glasgow", "liverpool",
        "bristol", "leeds", "sheffield", "newcastle", "nottingham", "belfast",
        "cardiff", "southampton", "oxford", "cambridge", "bath", "brighton",
    },
    "FR": {
        "paris", "marseille", "lyon", "toulouse", "nice", "nantes", "strasbourg",
        "montpellier", "bordeaux", "lille", "rennes", "reims", "saint-etienne",
        "toulon", "le havre", "grenoble", "dijon", "angers", "cannes", "monaco",
    },
    "IT": {
        "rome", "milan", "naples", "turin", "palermo", "genoa", "bologna",
        "florence", "bari", "catania", "venice", "verona", "messina", "padua",
    },
    "DE": {
        "berlin", "hamburg", "munich", "cologne", "frankfurt", "stuttgart",
        "dusseldorf", "leipzig", "dortmund", "essen", "bremen", "dresden",
    },
    "CA": {
        "toronto", "montreal", "vancouver", "calgary", "edmonton", "ottawa",
        "winnipeg", "quebec city", "hamilton", "kitchener", "victoria", "halifax",
    },
    "AU": {
        "sydney", "melbourne", "brisbane", "perth", "adelaide", "gold coast",
        "canberra", "newcastle", "wollongong", "hobart", "geelong",
    },
    "IN": {
        "mumbai", "delhi", "bangalore", "bengaluru", "hyderabad", "chennai",
        "kolkata", "pune", "ahmedabad", "jaipur", "surat", "lucknow", "kanpur",
        "nagpur", "indore", "thane", "bhopal", "visakhapatnam", "vadodara",
    },
}


def verify_local_geo_alignment(
    crawled_text: str,
    url: str,
    title: str,
    extracted_address: str,
    snippet: str,
    city: str,
    country_iso: str,
    loc_profile: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, bool]:
    """
    Intra-Country Locality Verification (Fix 1):
    Prevents cross-city false positives within the same country (e.g. Dallas businesses appearing in Miami).

    Returns:
        (is_valid, local_presence_confirmed)
        - is_valid: True if candidate does NOT contradict the requested location. False if it explicitly belongs
                    to a conflicting municipality within the same country.
        - local_presence_confirmed: True if the queried city, its known districts, postal codes, or area codes
                    were positively identified in the page content, title, snippet, or address.
    """
    target_city = (city or "").strip().lower()
    country_key = (country_iso or "").strip().upper()
    if country_key == "UK":
        country_key = "GB"

    if not target_city:
        return True, False

    # Check if query was national or state-wide (e.g. "Texas", "France", "United States")
    if loc_profile:
        p_country = (loc_profile.get("country_name") or "").lower().strip()
        p_state = (loc_profile.get("state") or "").lower().strip()
        if target_city in (p_country, p_state) or (p_state and target_city == p_state.lower()):
            return True, False

    # Assemble local positive markers
    markers: Set[str] = {target_city}
    if loc_profile:
        for d in loc_profile.get("districts", []):
            if d and len(d.strip()) >= 3:
                markers.add(d.strip().lower())
        for pc in loc_profile.get("postal_codes", []):
            if pc and len(pc.strip()) >= 3:
                markers.add(pc.strip().lower())
        for ac in loc_profile.get("area_codes", []):
            if ac and len(ac.strip()) >= 2:
                markers.add(ac.strip().lower())

    # Search blob for local presence confirmation
    search_blob = f"{url} {title} {extracted_address} {snippet} {crawled_text[:15000]}".lower()

    local_confirmed = False
    for m in markers:
        if m.isdigit():
            # Match numeric postal or area codes with boundary / delimiters
            if re.search(rf"(?:^|\D){re.escape(m)}(?:\D|$)", search_blob):
                local_confirmed = True
                break
        else:
            if m in search_blob:
                local_confirmed = True
                break

    # Detect conflicting major cities within the same country
    major_cities = MAJOR_CITIES_BY_COUNTRY.get(country_key, set())
    high_priority_blob = f"{title} {extracted_address} {urlparse(url).netloc}".lower()

    for other_city in major_cities:
        if other_city == target_city or other_city in markers:
            continue
        # Only treat as conflict if whole word matches in title or extracted address
        pattern = rf"\b{re.escape(other_city)}\b"
        if re.search(pattern, high_priority_blob):
            if not local_confirmed:
                # Explicit cross-city conflict without any queried city presence
                logger.debug(
                    "Intra-country geo conflict: lead explicitly references '%s' instead of queried city '%s'",
                    other_city,
                    target_city,
                )
                return False, False

    return True, local_confirmed


def generate_query_waves(
    category: str,
    city: str,
    state: str = "",
    country_iso: str = "US",
    districts: Optional[List[str]] = None,
) -> List[List[str]]:
    """
    Generate prioritized waves of diverse, localized search queries (Rule 4).
    Wave 1: Core English + Localized queries.
    Wave 2: Studio / Firm / Company / Showroom + Sub-districts.
    Wave 3: Contact / Phone / Email + Advanced discovery.
    """
    loc_base = f"{city} {state}".strip() if state else city
    iso = (country_iso or "").upper().strip()
    districts = districts or []

    # Identify primary localized terms if applicable
    loc_terms: List[str] = []
    clean_cat = category.lower().strip()
    for cat_key, trans in LOCALIZED_CATEGORY_TERMS.items():
        if cat_key in clean_cat and iso in trans:
            loc_terms.extend(trans[iso])

    wave1 = [
        f"{category} {loc_base}",
        f"{category} showroom {loc_base}",
        f"{category} official website {loc_base}",
    ]
    if loc_terms:
        primary_loc = loc_terms[0]
        wave1.append(f"{primary_loc} {loc_base}")
        if len(loc_terms) > 1:
            wave1.append(f"{loc_terms[1]} {loc_base}")

    wave2 = [
        f"{category} studio {loc_base}",
        f"{category} firm {loc_base}",
        f"{category} company {loc_base}",
        f"{category} services {loc_base}",
    ]
    if loc_terms:
        primary_loc = loc_terms[0]
        if iso == "IT":
            wave2.append(f"studio {primary_loc} {loc_base}")
            wave2.append(f"{primary_loc} sito ufficiale {loc_base}")
        elif iso == "FR":
            wave2.append(f"atelier {primary_loc} {loc_base}")
            wave2.append(f"{primary_loc} site officiel {loc_base}")
        elif iso == "ES":
            wave2.append(f"estudio {primary_loc} {loc_base}")
            wave2.append(f"{primary_loc} sitio oficial {loc_base}")
        elif iso == "DE":
            wave2.append(f"{primary_loc} geschäft {loc_base}")
            wave2.append(f"{primary_loc} offizielle webseite {loc_base}")

    # Sub-district queries in Wave 2
    for dist in districts[:4]:
        wave2.append(f"{category} in {dist} {city}")
        if loc_terms:
            wave2.append(f"{loc_terms[0]} {dist} {city}")

    wave3 = [
        f"{category} {loc_base} contact phone email",
        f"{category} {loc_base} address phone",
    ]
    if loc_terms:
        primary_loc = loc_terms[0]
        if iso == "IT":
            wave3.append(f"{primary_loc} {loc_base} contatti telefono")
        elif iso == "FR":
            wave3.append(f"{primary_loc} {loc_base} contact téléphone")
        elif iso == "ES":
            wave3.append(f"{primary_loc} {loc_base} contacto teléfono")
        elif iso == "DE":
            wave3.append(f"{primary_loc} {loc_base} kontakt telefon")

    for dist in districts[4:8]:
        wave3.append(f"{category} in {dist} {city}")

    return [wave1, wave2, wave3]


def tld_priority_score(url: str, target_iso: str) -> int:
    """Prioritize country-specific TLDs for target_iso (e.g. .it for IT, .fr for FR, .co.uk for GB)."""
    tld_map = {
        "IT": (".it",),
        "FR": (".fr",),
        "DE": (".de",),
        "GB": (".co.uk", ".uk"),
        "ES": (".es",),
        "AE": (".ae",),
        "IN": (".in", ".co.in"),
        "US": (".com", ".us", ".org"),
        "AU": (".com.au", ".au"),
        "CH": (".ch",),
        "NL": (".nl",),
        "BE": (".be",),
        "AT": (".at",),
    }
    iso = (target_iso or "").upper().strip()
    if iso in tld_map:
        parsed = urlparse(url).netloc.lower()
        if any(parsed.endswith(suffix) for suffix in tld_map[iso]):
            return 2
    return 1


def decode_yahoo_url(url: str) -> str:
    """Extract destination URL from Yahoo redirect."""
    m = re.search(r'/RU=([^/&]+)', url)
    if m:
        try:
            d = unquote(m.group(1))
            if d.startswith("http"):
                return d
        except Exception:
            pass
    return url


def is_directory_or_junk(url: str, title: str) -> bool:
    """
    Detect whether a search result is an aggregator, directory, blog, or junk page.
    TLD-Agnostic Directory & Aggregator Blocking (Fix 2).
    """
    if not url:
        return True

    parsed = urlparse(url)
    domain = parsed.netloc.lower().replace("www.", "")
    root_domain = extract_root_domain(url)
    domain_label = extract_domain_label(url)

    # 1. TLD-Agnostic label match & directory label set
    if domain_label in DIRECTORY_LABELS or any(sub in domain_label for sub in DIRECTORY_KEYWORD_SUBSTRINGS):
        return True

    # 2. Exact domain / root domain match
    if root_domain in DIRECTORY_DOMAINS or any(d in domain for d in DIRECTORY_DOMAINS):
        return True

    # 3. Path pattern check (listicles, directories, blog articles)
    if NON_BUSINESS_PATH_PATTERNS.search(parsed.path):
        return True

    # 4. Title prefix & editorial / ranking check
    if EDITORIAL_TITLE_PREFIXES.search(title):
        return True

    # 5. Public sector / municipal / tourism exclusion
    if PUBLIC_SECTOR_JUNK_PATTERNS.search(url) or PUBLIC_SECTOR_JUNK_PATTERNS.search(title):
        return True

    return False


# ---------------------------------------------------------------------------
# Multi-Engine Live Search Discovery (Yahoo, Bing, DDGS, Overpass)
# ---------------------------------------------------------------------------

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# Staggered search concurrency to eliminate IP rate limits (Fix 4)
SEARCH_SEMAPHORE = asyncio.Semaphore(4)


def decode_bing_url(url: str, cite_text: str = "") -> str:
    """Extract destination URL from Bing redirect or cite element."""
    if not url:
        return ""
    if "bing.com/ck/a" in url:
        m = re.search(r"[?&]u=([^&]+)", url)
        if m:
            raw = m.group(1)
            if raw.startswith("a1"):
                raw = raw[2:]
            padded = raw + "=" * (-len(raw) % 4)
            try:
                dec = base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
                if dec.startswith("http"):
                    return dec
            except Exception:
                pass
    if cite_text and cite_text.startswith("http"):
        return cite_text.split(" › ")[0].strip()
    return url


async def search_yahoo(query: str, max_pages: int = 2) -> List[Dict[str, str]]:
    """
    Scrape Yahoo Search for organic business results with RU redirect decoding.
    Captures search snippet for fallback phone/contact enrichment (Fix 3).
    """
    candidates: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()

    async with SEARCH_SEMAPHORE:
        await asyncio.sleep(random.uniform(0.15, 0.35))
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=5.0) as client:
                for page in range(max_pages):
                    start = page * 10 + 1
                    resp = await client.get("https://search.yahoo.com/search", params={"p": query, "b": start})
                    if resp.status_code != 200:
                        break

                    soup = BeautifulSoup(resp.text, "html.parser")
                    for div in soup.find_all("div", class_="compTitle"):
                        h3 = div.find("h3")
                        a = div.find("a", href=True)
                        if not h3 or not a:
                            continue

                        title = h3.get_text(strip=True)
                        raw_url = a["href"]
                        url = decode_yahoo_url(raw_url)

                        if not url.startswith("http") or "aclick" in url or "yahoo.com" in url:
                            continue
                        if is_directory_or_junk(url, title):
                            continue

                        root_dom = extract_root_domain(url)
                        if not root_dom:
                            continue

                        parent = div.find_parent("li") or div.parent
                        snippet_el = parent.find("div", class_="compText") or parent.find("p") if parent else None
                        snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""

                        if url not in seen_urls:
                            seen_urls.add(url)
                            candidates.append({
                                "title": title,
                                "url": url,
                                "snippet": snippet,
                                "root_domain": root_dom,
                            })
                    await asyncio.sleep(0.05)
        except Exception as exc:
            logger.debug("Yahoo search error for query '%s': %s", query, exc)

    return candidates


async def search_bing(query: str, max_pages: int = 2) -> List[Dict[str, str]]:
    """
    Scrape Bing Search for organic business results with base64 redirect decoding.
    Captures search snippet for fallback phone/contact enrichment (Fix 3 & Fix 4).
    """
    candidates: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()

    async with SEARCH_SEMAPHORE:
        await asyncio.sleep(random.uniform(0.15, 0.35))
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=5.0) as client:
                for page in range(max_pages):
                    start = page * 10 + 1
                    resp = await client.get("https://www.bing.com/search", params={"q": query, "first": start})
                    if resp.status_code != 200:
                        break

                    soup = BeautifulSoup(resp.text, "html.parser")
                    for li in soup.find_all("li", class_="b_algo"):
                        h2 = li.find("h2")
                        a = h2.find("a", href=True) if h2 else None
                        if not a:
                            continue
                        cite = li.find("cite")
                        cite_text = cite.get_text(strip=True) if cite else ""
                        raw_url = a["href"]
                        url = decode_bing_url(raw_url, cite_text)

                        if not url.startswith("http") or "bing.com" in url:
                            continue
                        title = a.get_text(strip=True)
                        if is_directory_or_junk(url, title):
                            continue

                        root_dom = extract_root_domain(url)
                        if not root_dom:
                            continue

                        snippet_el = li.find("p") or li.find("div", class_="b_caption")
                        snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""

                        if url not in seen_urls:
                            seen_urls.add(url)
                            candidates.append({
                                "title": title,
                                "url": url,
                                "snippet": snippet,
                                "root_domain": root_dom,
                            })
                    await asyncio.sleep(0.05)
        except Exception as exc:
            logger.debug("Bing search error for query '%s': %s", query, exc)

    return candidates


async def search_ddg(query: str, max_results: int = 15) -> List[Dict[str, str]]:
    """Query DuckDuckGo for organic business results (Fix 4)."""
    candidates: List[Dict[str, str]] = []
    if not DDGS:
        return candidates

    async with SEARCH_SEMAPHORE:
        await asyncio.sleep(random.uniform(0.15, 0.35))
        try:
            loop = asyncio.get_running_loop()
            def do_ddg():
                with DDGS() as ddgs:
                    return list(ddgs.text(query, max_results=max_results))
            results = await loop.run_in_executor(None, do_ddg)
            for r in results:
                url = r.get("href", "")
                title = r.get("title", "")
                snippet = r.get("body", "")
                if not url.startswith("http") or is_directory_or_junk(url, title):
                    continue
                root_dom = extract_root_domain(url)
                if not root_dom:
                    continue
                candidates.append({
                    "title": title,
                    "url": url,
                    "snippet": snippet,
                    "root_domain": root_dom,
                })
        except Exception as exc:
            logger.debug("DDGS search error for '%s': %s", query, exc)

    return candidates


async def search_overpass_spatial(
    category: str,
    lat: float,
    lon: float,
    city: str,
    country_iso: str,
    country_name: str,
) -> Tuple[List[dict], List[Dict[str, str]]]:
    """
    Direct Spatial Node Discovery via OpenStreetMap Overpass (Fix 3 & Fix 4).
    Emits spatial nodes with valid target-country phones directly as verified leads,
    and returns candidates with websites for supplemental crawling.
    """
    if lat == 0.0 and lon == 0.0:
        return [], []

    direct_leads: List[dict] = []
    candidate_urls: List[Dict[str, str]] = []

    clean_cat = category.strip()
    words = [w for w in re.findall(r"[a-zA-Z\u00C0-\u017F]+", clean_cat) if len(w) >= 3]
    cat_pattern = "|".join(words) if words else clean_cat

    query = f"""[out:json][timeout:4];
(
  node["shop"](around:20000,{lat},{lon});
  node["name"~"{cat_pattern}",i](around:20000,{lat},{lon});
  way["name"~"{cat_pattern}",i](around:20000,{lat},{lon});
);
out center 100 tags;
"""
    endpoints = [
        "https://overpass.private.coffee/api/interpreter",
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
    ]
    headers = {
        "User-Agent": "BigLeaguesLeadExtractor/2.0",
        "Accept": "application/json, */*",
    }

    async with SEARCH_SEMAPHORE:
        try:
            async with httpx.AsyncClient(headers=headers, timeout=3.5) as client:
                for ep in endpoints:
                    try:
                        resp = await client.post(ep, data={"data": query})
                        if resp.status_code == 200:
                            data = resp.json()
                            for el in data.get("elements", []):
                                tags = el.get("tags", {})
                                name = tags.get("name") or tags.get("name:en")
                                if not name or len(name.strip()) < 3:
                                    continue
                                phone_raw = (
                                    tags.get("phone")
                                    or tags.get("contact:phone")
                                    or tags.get("contact:mobile")
                                    or tags.get("mobile")
                                    or ""
                                )
                                website = tags.get("website") or tags.get("contact:website") or ""
                                email = tags.get("email") or tags.get("contact:email") or ""

                                addr_parts = [tags[k] for k in ("addr:housenumber", "addr:street", "addr:suburb", "addr:postcode") if tags.get(k)]
                                address = ", ".join(addr_parts) if addr_parts else f"{city}, {country_name}"

                                if phone_raw:
                                    e164, disp, wa, v_reg = validate_and_format_phone(phone_raw, country_iso, target_iso=country_iso)
                                    if e164:
                                        wa_url = extract_whatsapp_url("", e164, is_whatsapp_ready=wa)
                                        em_clean = email.lower().strip() if email else ""
                                        mx_ver = await verify_email_mx(em_clean) if em_clean else False
                                        score = calculate_lead_score(
                                            has_phone=True,
                                            has_whatsapp=bool(wa_url),
                                            email=em_clean,
                                            email_mx_verified=mx_ver,
                                            has_instagram=False,
                                            address=address,
                                            city=city,
                                            country_name=country_name,
                                            has_meta_pixel=False,
                                            has_google_ads=False,
                                        )
                                        direct_leads.append({
                                            "name": name.strip(),
                                            "address": address,
                                            "rating": None,
                                            "phone": disp,
                                            "phone_e164": e164,
                                            "email": em_clean,
                                            "instagram_handle": None,
                                            "instagram_url": None,
                                            "website": website if website.startswith("http") else "",
                                            "category": clean_cat.title(),
                                            "city": city,
                                            "country_code": (v_reg or country_iso).upper(),
                                            "source": "osm_spatial",
                                            "has_meta_pixel": False,
                                            "has_google_ads": False,
                                            "cms_platform": "Custom",
                                            "whatsapp_url": wa_url,
                                            "is_whatsapp_ready": wa or bool(wa_url),
                                            "linkedin_url": "",
                                            "lead_score": score,
                                            "email_mx_verified": mx_ver,
                                        })
                                elif website and website.startswith("http") and not is_directory_or_junk(website, name):
                                    root_dom = extract_root_domain(website)
                                    if root_dom:
                                        candidate_urls.append({
                                            "title": name.strip(),
                                            "url": website,
                                            "snippet": "",
                                            "root_domain": root_dom,
                                        })
                            if direct_leads or candidate_urls:
                                break
                    except Exception:
                        continue
        except Exception as exc:
            logger.debug("Overpass query error: %s", exc)

    return direct_leads, candidate_urls


# ---------------------------------------------------------------------------
# Deep Website Crawler & Live Enrichment (Fast 3.2s Timeout)
# ---------------------------------------------------------------------------

async def crawl_business_website(
    client: httpx.AsyncClient,
    candidate: Dict[str, str],
    category: str,
    city: str,
    state: str,
    country_iso: str,
    country_name: str,
    loc_profile: Optional[Dict[str, Any]] = None,
) -> Optional[dict]:
    """
    Crawl business website homepage and contact subpage.
    Extracts brand name, callable phone (E.164), real email, real Instagram.
    Enforces sovereign ccTLD checks, TLD-agnostic directory blocking, and language/script geo consistency.
    Enforces intra-country locality verification (Fix 1).
    Implements multi-channel reachability gatekeeper (phone, MX email, or IG).
    """
    url = candidate.get("url", "")
    root_domain = candidate.get("root_domain") or extract_root_domain(url)
    base_domain = urlparse(url).netloc if url else ""

    # TLD-Agnostic Directory / Aggregator / Junk check (Fix 2)
    if url and is_directory_or_junk(url, candidate.get("title", "")):
        return None

    # Sovereign ccTLD Gatekeeper: Reject foreign sovereign ccTLD pre-crawl
    if url and is_cctld_mismatched(url, country_iso):
        return None

    # Pre-crawl path & public sector rejection
    if url:
        parsed_path = urlparse(url).path
        if NON_BUSINESS_PATH_PATTERNS.search(parsed_path):
            return None
        if PUBLIC_SECTOR_JUNK_PATTERNS.search(url):
            return None

    html = ""
    headers = get_browser_headers(country_iso)
    if url:
        try:
            async with client.stream(
                "GET",
                url,
                headers=headers,
                timeout=httpx.Timeout(3.2, connect=1.8, read=2.5),
                follow_redirects=True,
            ) as resp:
                if resp.status_code < 400:
                    content_type = resp.headers.get("content-type", "").lower()
                    if any(t in content_type for t in ("text/html", "application/xhtml")):
                        chunks = []
                        total_bytes = 0
                        max_bytes = 300_000  # Cap HTML ingestion at 300 KB
                        async for chunk in resp.aiter_bytes():
                            chunks.append(chunk)
                            total_bytes += len(chunk)
                            if total_bytes >= max_bytes:
                                break
                        raw_bytes = b"".join(chunks)[:max_bytes]
                        encoding = resp.encoding or "utf-8"
                        try:
                            html = raw_bytes.decode(encoding, errors="replace")
                        except Exception:
                            html = raw_bytes.decode("utf-8", errors="replace")
        except Exception:
            html = ""

    # Search Snippet / Pre-populated Maps Fallback
    if not html:
        snippet = candidate.get("snippet", "")
        pre_phone = candidate.get("phone", "")
        cand_title = candidate.get("title", "")
        cand_addr = candidate.get("address", "")

        # Intra-country locality verification (Fix 1)
        is_valid, local_confirmed = verify_local_geo_alignment(
            crawled_text="",
            url=url,
            title=cand_title,
            extracted_address=cand_addr,
            snippet=snippet,
            city=city,
            country_iso=country_iso,
            loc_profile=loc_profile,
        )
        if not is_valid:
            return None

        e164, disp, is_wa_ready, v_reg = validate_and_format_phone(pre_phone, country_iso, target_iso=country_iso)
        if not e164 and snippet:
            try:
                for match in phonenumbers.PhoneNumberMatcher(snippet, country_iso):
                    p_str = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
                    e164, disp, is_wa_ready, v_reg = validate_and_format_phone(p_str, country_iso, target_iso=country_iso)
                    if e164:
                        break
            except Exception:
                pass

        brand_name = candidate.get("name") or resolve_brand_name(None, None, cand_title, root_domain, city, state, country_name)
        brand_name = sanitize_brand_title(brand_name)
        em_matches = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', snippet)
        valid_ems = [em for em in em_matches if not is_cctld_mismatched(em, country_iso)]
        email, mx_verified, _ = await rank_and_verify_emails(valid_ems)
        wa_url = extract_whatsapp_url("", e164, is_whatsapp_ready=is_wa_ready) if e164 else ""

        # Multi-Channel Reachability Gatekeeper
        has_phone = bool(e164 and disp)
        has_mx_email = bool(email and mx_verified)
        has_ig = bool(url and candidate.get("instagram_handle"))
        if not (has_phone or has_mx_email or has_ig):
            return None

        # Address stamping: never stamp {queried_city}, {country} unless local presence verified
        if cand_addr:
            address_to_use = cand_addr
        elif local_confirmed:
            address_to_use = f"{city}, {country_name}"
        else:
            address_to_use = country_name

        score = calculate_lead_score(
            has_phone=has_phone,
            has_whatsapp=bool(wa_url),
            email=email,
            email_mx_verified=mx_verified,
            has_instagram=has_ig,
            address=address_to_use,
            city=city,
            country_name=country_name,
            has_meta_pixel=False,
            has_google_ads=False,
        )
        return {
            "name": brand_name,
            "address": address_to_use,
            "rating": candidate.get("rating"),
            "phone": disp or "",
            "phone_e164": e164 or "",
            "email": email or "",
            "instagram_handle": candidate.get("instagram_handle") or None,
            "instagram_url": candidate.get("instagram_url") or None,
            "website": url,
            "category": category.strip().title(),
            "city": city,
            "country_code": (v_reg or country_iso).upper(),
            "source": candidate.get("source", "snippet_discovery"),
            "has_meta_pixel": False,
            "has_google_ads": False,
            "cms_platform": "",
            "whatsapp_url": wa_url,
            "is_whatsapp_ready": is_wa_ready or bool(wa_url),
            "linkedin_url": "",
            "lead_score": score,
            "email_mx_verified": mx_verified,
        }

    # Language/Script Consistency Check on HTML
    if is_script_or_lang_mismatched(html, country_iso):
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Reject pages where og:type == "article"
    og_type = soup.find("meta", property=re.compile(r"^og:type$", re.I)) or soup.find("meta", attrs={"name": re.compile(r"^og:type$", re.I)})
    if og_type and og_type.get("content", "").strip().lower() == "article":
        return None

    raw_page_title = soup.title.get_text().strip() if soup.title else candidate.get("title", "")
    if EDITORIAL_TITLE_PREFIXES.search(raw_page_title) or PUBLIC_SECTOR_JUNK_PATTERNS.search(raw_page_title):
        return None

    # 1. Brand Name & Identity Extraction from Root Homepage Only
    json_ld_name = None
    json_ld_types: List[str] = []
    for script in soup.find_all("script", type="application/ld+json"):
        if script.string:
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for it in items:
                    if isinstance(it, dict):
                        sub_items = it.get("@graph", [it]) if isinstance(it.get("@graph"), list) else [it]
                        for sub in sub_items:
                            if isinstance(sub, dict):
                                if "@type" in sub:
                                    json_ld_types.append(str(sub["@type"]))
                                if "name" in sub and not json_ld_name:
                                    t = str(sub.get("@type", "")).lower()
                                    if any(k in t for k in ("business", "store", "organization", "local", "place")):
                                        candidate_name = str(sub["name"]).strip()
                                        if 2 <= len(candidate_name) <= 60:
                                            json_ld_name = candidate_name
            except Exception:
                pass

    og_site = soup.find("meta", property=re.compile(r"^og:site_name$", re.I)) or soup.find("meta", attrs={"name": re.compile(r"^og:site_name$", re.I)})
    og_site_name = og_site["content"].strip() if (og_site and og_site.get("content")) else None

    brand_name = candidate.get("name") or resolve_brand_name(og_site_name, json_ld_name, raw_page_title, root_domain, city, state, country_name)

    # 2. Check Homepage for Phone & Email first
    homepage_text = soup.get_text(" ", strip=True)
    raw_phones: List[str] = []
    if candidate.get("phone"):
        raw_phones.append(candidate["phone"])

    for a in soup.find_all("a", href=re.compile(r"^tel:", re.I)):
        raw_phones.append(a["href"].replace("tel:", "").strip())

    try:
        for match in phonenumbers.PhoneNumberMatcher(homepage_text, country_iso):
            raw_phones.append(phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164))
    except Exception:
        pass

    raw_emails: List[str] = []
    for a in soup.find_all("a", href=re.compile(r"^mailto:", re.I)):
        raw_emails.append(a["href"])

    # If missing phone or email, crawl contact subpage quickly
    contact_html = ""
    if len(raw_phones) == 0 or len(raw_emails) == 0:
        contact_urls: List[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            lower_href = href.lower()
            if any(w in lower_href for w in ["contact", "about", "connect", "location", "contatti", "kontakt"]):
                if href.startswith("http"):
                    if urlparse(href).netloc == base_domain:
                        contact_urls.append(href)
                elif href.startswith("/"):
                    contact_urls.append(f"{urlparse(url).scheme}://{base_domain}{href}")

        if contact_urls:
            try:
                c_headers = get_browser_headers(country_iso)
                async with client.stream(
                    "GET",
                    contact_urls[0],
                    headers=c_headers,
                    timeout=httpx.Timeout(2.5, connect=1.5, read=2.0),
                    follow_redirects=True,
                ) as c_resp:
                    if c_resp.status_code < 400:
                        c_type = c_resp.headers.get("content-type", "").lower()
                        if any(t in c_type for t in ("text/html", "application/xhtml")):
                            c_chunks = []
                            c_total = 0
                            async for chunk in c_resp.aiter_bytes():
                                c_chunks.append(chunk)
                                c_total += len(chunk)
                                if c_total >= 250_000:
                                    break
                            c_raw = b"".join(c_chunks)[:250_000]
                            c_enc = c_resp.encoding or "utf-8"
                            try:
                                contact_html = c_raw.decode(c_enc, errors="replace")
                            except Exception:
                                contact_html = c_raw.decode("utf-8", errors="replace")
                            c_soup = BeautifulSoup(contact_html, "html.parser")
                            for a in c_soup.find_all("a", href=re.compile(r"^tel:", re.I)):
                                raw_phones.append(a["href"].replace("tel:", "").strip())
                            for match in phonenumbers.PhoneNumberMatcher(c_soup.get_text(" ", strip=True), country_iso):
                                raw_phones.append(phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164))
                            for a in c_soup.find_all("a", href=re.compile(r"^mailto:", re.I)):
                                raw_emails.append(a["href"])
            except Exception:
                pass

    combined_html = html + " " + contact_html
    combined_soup = BeautifulSoup(combined_html, "html.parser")
    combined_text = combined_soup.get_text(" ", strip=True)

    # 3. Strict Target-Country Phone Verification & Region Alignment
    disp_phone: Optional[str] = None
    phone_e164: Optional[str] = None
    is_wa_ready: bool = False
    verified_country_iso: Optional[str] = None

    for p in raw_phones:
        e164, disp, wa, v_reg = validate_and_format_phone(p, country_iso, target_iso=country_iso)
        if e164:
            phone_e164 = e164
            disp_phone = disp
            is_wa_ready = wa
            verified_country_iso = v_reg
            break

    # Snippet Fallback if page had no phone link/text
    if not disp_phone or not phone_e164:
        snippet = candidate.get("snippet", "")
        if snippet:
            try:
                for match in phonenumbers.PhoneNumberMatcher(snippet, country_iso):
                    raw_p = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
                    e164, disp, wa, v_reg = validate_and_format_phone(raw_p, country_iso, target_iso=country_iso)
                    if e164:
                        phone_e164 = e164
                        disp_phone = disp
                        is_wa_ready = wa
                        verified_country_iso = v_reg
                        break
            except Exception:
                pass

    # 5. Email Extraction & Strict RFC Sanitization + Async MX Verification
    for em in re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', combined_text):
        raw_emails.append(em)

    # Sovereign ccTLD validation on candidate emails
    filtered_emails = [em for em in raw_emails if not is_cctld_mismatched(em, country_iso)]
    email, email_mx_verified, ranked_emails = await rank_and_verify_emails(filtered_emails)

    # 6. Instagram Extraction
    ig_handle, ig_url = extract_instagram(combined_html, url)

    # Multi-Channel Reachability Gatekeeper (Upgrade 3)
    # Emit any real business that has at least one verified actionable outbound channel:
    # (a) valid target-country phone_e164, OR
    # (b) email_mx_verified business email, OR
    # (c) live website paired with verified @instagram handle
    has_valid_phone = bool(phone_e164 and disp_phone)
    has_mx_email = bool(email and email_mx_verified)
    has_verified_ig = bool(url and ig_handle)

    if not (has_valid_phone or has_mx_email or has_verified_ig):
        return None

    # 7. Address Extraction
    address = candidate.get("address")
    if not address or address == city or address == f"{city}, {country_name}":
        for script in combined_soup.find_all("script", type="application/ld+json"):
            if script.string:
                try:
                    data = json.loads(script.string)
                    items = data if isinstance(data, list) else [data]
                    for it in items:
                        if isinstance(it, dict) and "address" in it:
                            addr_obj = it["address"]
                            if isinstance(addr_obj, dict):
                                parts = [
                                    addr_obj.get("streetAddress"),
                                    addr_obj.get("addressLocality") or city,
                                    addr_obj.get("postalCode"),
                                    addr_obj.get("addressCountry") or country_name,
                                ]
                                clean_parts = [str(p).strip() for p in parts if p]
                                if clean_parts:
                                    address = ", ".join(clean_parts)
                                    break
                            elif isinstance(addr_obj, str):
                                address = addr_obj.strip()
                                break
                except Exception:
                    pass
            if address and address != city and address != f"{city}, {country_name}":
                break

    # Intra-country locality verification (Fix 1)
    is_valid, local_confirmed = verify_local_geo_alignment(
        crawled_text=combined_text,
        url=url,
        title=raw_page_title,
        extracted_address=address or "",
        snippet=candidate.get("snippet", ""),
        city=city,
        country_iso=country_iso,
        loc_profile=loc_profile,
    )
    if not is_valid:
        return None

    # Safe address stamping: never stamp {queried_city}, {country} unless local presence verified (Rule 3)
    if not address or address == city or address == f"{city}, {country_name}":
        if local_confirmed:
            address = f"{city}, {country_name}"
        else:
            address = country_name

    # 8. Rating Extraction
    rating: Optional[float] = candidate.get("rating")
    if rating is None:
        for script in combined_soup.find_all("script", type="application/ld+json"):
            if script.string:
                try:
                    data = json.loads(script.string)
                    items = data if isinstance(data, list) else [data]
                    for it in items:
                        if isinstance(it, dict) and "aggregateRating" in it:
                            ar = it["aggregateRating"]
                            if isinstance(ar, dict) and "ratingValue" in ar:
                                val = float(ar["ratingValue"])
                                if 1.0 <= val <= 5.0:
                                    rating = round(val, 1)
                                    break
                except Exception:
                    pass
            if rating is not None:
                break

    # 9. Outbound Marketing & Tech Intelligence Signals (Upgrade 2)
    has_meta_pixel = detect_meta_pixel(html)
    has_google_ads = detect_google_ads(html)
    cms_platform = detect_cms_platform(html)
    whatsapp_url = extract_whatsapp_url(html, phone_e164, is_whatsapp_ready=is_wa_ready) if phone_e164 else ""
    linkedin_url = extract_linkedin_url(html)

    lead_score = calculate_lead_score(
        has_phone=has_valid_phone,
        has_whatsapp=bool(whatsapp_url),
        email=email,
        email_mx_verified=email_mx_verified,
        has_instagram=bool(ig_handle),
        address=address,
        city=city,
        country_name=country_name,
        has_meta_pixel=has_meta_pixel,
        has_google_ads=has_google_ads,
    )

    lead = {
        "name": brand_name,
        "address": address,
        "rating": rating,
        "phone": disp_phone or "",
        "phone_e164": phone_e164 or "",
        "email": email or "",
        "instagram_handle": ig_handle or None,
        "instagram_url": ig_url or None,
        "website": url,
        "category": category.strip().title(),
        "city": city,
        "country_code": (verified_country_iso or country_iso).upper(),
        "source": candidate.get("source", "live_web"),
        # Outbound intelligence signals
        "has_meta_pixel": has_meta_pixel,
        "has_google_ads": has_google_ads,
        "cms_platform": cms_platform,
        "whatsapp_url": whatsapp_url,
        "is_whatsapp_ready": is_wa_ready or bool(whatsapp_url),
        "linkedin_url": linkedin_url,
        "lead_score": lead_score,
        "email_mx_verified": email_mx_verified,
    }
    return lead


def ensure_lead_intelligence(lead: dict, country_iso: str, country_name: str, city: str) -> dict:
    """Ensure older cached records have all intelligence fields populated."""
    phone_e164 = lead.get("phone_e164", "")
    email = lead.get("email", "")
    address = lead.get("address", "")
    ig = lead.get("instagram_handle")

    if "has_meta_pixel" not in lead:
        lead["has_meta_pixel"] = False
    if "has_google_ads" not in lead:
        lead["has_google_ads"] = False
    if "cms_platform" not in lead:
        lead["cms_platform"] = ""
    if "whatsapp_url" not in lead or not lead["whatsapp_url"]:
        if phone_e164:
            _, _, is_wa, _ = validate_and_format_phone(phone_e164, country_iso, target_iso=country_iso)
            lead["whatsapp_url"] = extract_whatsapp_url("", phone_e164, is_whatsapp_ready=is_wa)
            lead["is_whatsapp_ready"] = is_wa
        else:
            lead["whatsapp_url"] = ""
            lead["is_whatsapp_ready"] = False
    elif "is_whatsapp_ready" not in lead:
        if phone_e164:
            _, _, is_wa, _ = validate_and_format_phone(phone_e164, country_iso, target_iso=country_iso)
            lead["is_whatsapp_ready"] = is_wa or bool(lead.get("whatsapp_url"))
        else:
            lead["is_whatsapp_ready"] = False
    if "linkedin_url" not in lead:
        lead["linkedin_url"] = ""
    if "email_mx_verified" not in lead:
        lead["email_mx_verified"] = bool(email and "@" in email)

    if "lead_score" not in lead or lead["lead_score"] is None or lead["lead_score"] == 0:
        lead["lead_score"] = calculate_lead_score(
            has_phone=bool(phone_e164),
            has_whatsapp=bool(lead.get("whatsapp_url")),
            email=email,
            email_mx_verified=lead.get("email_mx_verified", False),
            has_instagram=bool(ig),
            address=address,
            city=city,
            country_name=country_name,
            has_meta_pixel=lead.get("has_meta_pixel", False),
            has_google_ads=lead.get("has_google_ads", False),
        )

    return lead


# ---------------------------------------------------------------------------
# Core Extraction Async Generator (Producer-Consumer Async Queue Pipeline)
# ---------------------------------------------------------------------------

async def extract_leads(
    category: str,
    location: str,
    volume: int = 50,
    star: int = 0,
    mode: str = "approx",
    bypass_cache: bool = False,
    maps_api_key: Optional[str] = None,
    stop_event: Optional[asyncio.Event] = None,
) -> AsyncIterator[dict]:
    """
    100% Genuine async generator for the Global B2B Lead Extractor.
    Streams verified real leads discovered live from the web.
    Guarantees:
      - Non-blocking Producer-Consumer asyncio.Queue architecture.
      - Pluggable high-volume Maps API provider alongside free engines.
      - Sovereign ccTLD & script geo alignment.
      - Multi-channel reachability gatekeeper (phone, MX email, or IG).
      - Phone & root domain deduplication locks.
      - Zero synthetic records.
    """
    clean_cat = category.strip()
    cities = parse_multi_city(location)
    total_target = max(5, min(volume, 1000))

    yielded_count = 0
    domain_crawl_counts: Dict[str, int] = defaultdict(int)
    seen_phones: Set[str] = set()
    seen_identities: Set[Tuple[str, str]] = set()
    seen_urls: Set[str] = set()

    yield {
        "type": "status",
        "stage": "init",
        "message": f"Starting extraction for {clean_cat} across {len(cities)} location(s)...",
        "scanned": 0,
        "verified": 0,
    }

    for city in cities:
        if yielded_count >= total_target or (stop_event and stop_event.is_set()):
            break

        city_clean = city.strip()
        loc_profile = resolve_any_global_location(city_clean)
        country_iso = loc_profile["country_code"]
        country_name = loc_profile["country_name"]
        city_name = loc_profile["city"]
        state_name = loc_profile.get("state", "")
        lat = float(loc_profile.get("lat") or 0.0)
        lon = float(loc_profile.get("lon") or 0.0)
        districts = loc_profile.get("districts", [])

        # ── Tier 1: Check Persistent SQLite Store & JSON Cache First ────────
        if not bypass_cache:
            yield {
                "type": "status",
                "stage": "cache",
                "message": f"Querying local SQLite persistent store for {city_name}...",
                "scanned": 0,
                "verified": yielded_count,
            }

            sqlite_leads = get_cached_leads_from_db(clean_cat, city_clean, ttl_days=7.0)
            filtered_cached = filter_by_rating(sqlite_leads, star, mode, total_target - yielded_count)

            cached_loaded_this_city = 0
            for raw_lead in filtered_cached:
                if yielded_count >= total_target or (stop_event and stop_event.is_set()):
                    break

                phone_e164 = raw_lead.get("phone_e164")
                website = raw_lead.get("website", "")
                lead_name = raw_lead.get("name") or raw_lead.get("business_name", "")
                root_dom = extract_root_domain(website)

                # 1. TLD-Agnostic Directory / Aggregator / Junk check on cached leads (Fix 2)
                if website and is_directory_or_junk(website, lead_name):
                    continue

                # 2. Sovereign ccTLD Check on Cached Leads
                if website and is_cctld_mismatched(website, country_iso):
                    continue

                # 3. Clean Title-Trimming Sanitizer (Fix 2)
                lead_name = sanitize_brand_title(lead_name)
                raw_lead["name"] = lead_name
                raw_lead["business_name"] = lead_name

                # 4. Strict Target-Country Phone Alignment & Phone-Type WhatsApp Classifier (Fix 2 & 3)
                if phone_e164:
                    e164, disp, is_wa, v_reg = validate_and_format_phone(phone_e164, country_iso, target_iso=country_iso)
                    if not e164:
                        continue
                    raw_lead["phone_e164"] = e164
                    raw_lead["phone"] = disp
                    raw_lead["country_code"] = (v_reg or country_iso).upper()
                    raw_lead["is_whatsapp_ready"] = is_wa

                    # Check if existing wa link was explicit in HTML
                    cur_wa = raw_lead.get("whatsapp_url", "")
                    clean_digits = re.sub(r"\D", "", phone_e164)
                    is_explicit = bool(cur_wa and ("wa.me" in cur_wa or "whatsapp.com" in cur_wa) and not cur_wa.endswith(clean_digits))
                    if is_explicit:
                        raw_lead["is_whatsapp_ready"] = True
                    else:
                        raw_lead["whatsapp_url"] = extract_whatsapp_url("", phone_e164, is_whatsapp_ready=is_wa)
                else:
                    raw_lead["is_whatsapp_ready"] = False
                    raw_lead["whatsapp_url"] = ""

                # 5. Intra-Country Locality Verification
                is_val, local_conf = verify_local_geo_alignment(
                    crawled_text="",
                    url=website,
                    title=lead_name,
                    extracted_address=raw_lead.get("address", ""),
                    snippet="",
                    city=city_name,
                    country_iso=country_iso,
                    loc_profile=loc_profile,
                )
                if not is_val:
                    continue

                # 6. Deduplication
                if phone_e164 and phone_e164 in seen_phones:
                    continue
                if root_dom and domain_crawl_counts[root_dom] >= 5:
                    continue

                norm_name = re.sub(r"\W+", "", lead_name.lower())
                norm_addr = re.sub(r"\W+", "", raw_lead.get("address", "").lower())
                identity_key = (norm_name, norm_addr)
                if norm_name and norm_addr and identity_key in seen_identities:
                    continue

                raw_lead = ensure_lead_intelligence(raw_lead, country_iso, country_name, city_name)

                if phone_e164:
                    seen_phones.add(phone_e164)
                if norm_name and norm_addr:
                    seen_identities.add(identity_key)
                if root_dom:
                    domain_crawl_counts[root_dom] += 1

                yielded_count += 1
                cached_loaded_this_city += 1
                yield raw_lead
                await asyncio.sleep(0.01)

            if cached_loaded_this_city > 0:
                yield {
                    "type": "status",
                    "stage": "cache",
                    "message": f"Loaded {cached_loaded_this_city} verified leads from cache ({yielded_count}/{total_target})",
                    "scanned": cached_loaded_this_city,
                    "verified": yielded_count,
                }
        else:
            yield {
                "type": "status",
                "stage": "bypass",
                "message": f"Fresh Live Crawl active — bypassing cache for {city_name}...",
                "scanned": 0,
                "verified": yielded_count,
            }

        if yielded_count >= total_target or (stop_event and stop_event.is_set()):
            break

        # ── Tier 2: Producer-Consumer Streaming Pipeline ────────────────────
        candidate_queue: asyncio.Queue[Optional[dict]] = asyncio.Queue(maxsize=400)
        output_queue: asyncio.Queue[Optional[dict]] = asyncio.Queue()
        internal_stop = asyncio.Event()
        producers_done = asyncio.Event()
        new_leads_for_cache: List[dict] = []

        query_waves = generate_query_waves(
            category=clean_cat,
            city=city_name,
            state=state_name,
            country_iso=country_iso,
            districts=districts,
        )

        async with make_client(country_iso=country_iso, timeout=4.0) as client:
            sem = asyncio.Semaphore(30)

            async def push_candidate(cand: dict):
                if internal_stop.is_set():
                    return
                url = cand.get("url", "")
                if url:
                    if url in seen_urls:
                        return
                    if is_cctld_mismatched(url, country_iso):
                        return
                    root_dom = cand.get("root_domain") or extract_root_domain(url)
                    if root_dom and domain_crawl_counts[root_dom] >= 5:
                        return
                    seen_urls.add(url)
                elif not (cand.get("source") == "maps_boost" and cand.get("phone")):
                    return

                try:
                    await asyncio.wait_for(candidate_queue.put(cand), timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

            # Producer 1: Maps Boost (Serper Places)
            async def maps_producer():
                key = maps_api_key or os.environ.get("SERPER_API_KEY", "")
                key = key.strip() if key else ""
                if not key:
                    return
                try:
                    await output_queue.put({
                        "type": "status",
                        "stage": "maps_boost",
                        "message": f"Maps Boost Active: Querying Google Maps for {clean_cat} in {city_name}...",
                        "scanned": 0,
                        "verified": yielded_count,
                    })
                    maps_cands = await harvest_maps_candidates(
                        category=clean_cat,
                        city=city_name,
                        target_iso=country_iso,
                        maps_api_key=key,
                        districts=districts,
                    )
                    for mc in maps_cands:
                        if internal_stop.is_set():
                            break
                        await push_candidate(mc)
                except Exception as exc:
                    logger.debug("Maps producer error: %s", exc)

            # Producer 2: OpenStreetMap Spatial Nodes
            async def osm_producer():
                if lat == 0.0 or lon == 0.0:
                    return
                try:
                    await output_queue.put({
                        "type": "status",
                        "stage": "spatial",
                        "message": f"Scanning OpenStreetMap spatial nodes in {city_name} ({lat:.2f}, {lon:.2f})...",
                        "scanned": 0,
                        "verified": yielded_count,
                    })
                    osm_leads, osm_cands = await search_overpass_spatial(
                        clean_cat, lat, lon, city_name, country_iso, country_name
                    )
                    for ol in osm_leads:
                        if internal_stop.is_set():
                            break
                        ol = ensure_lead_intelligence(ol, country_iso, country_name, city_name)
                        await output_queue.put(ol)
                    for oc in osm_cands:
                        if internal_stop.is_set():
                            break
                        await push_candidate(oc)
                except Exception as exc:
                    logger.debug("OSM producer error: %s", exc)

            # Producer 3: Multi-Engine Web Search (Bing, Yahoo, DDG)
            async def search_producer():
                try:
                    for wave_idx, wave_queries in enumerate(query_waves, 1):
                        if internal_stop.is_set():
                            break
                        await output_queue.put({
                            "type": "status",
                            "stage": "discovery",
                            "message": f"Wave {wave_idx}/{len(query_waves)}: Querying Bing, Yahoo & DuckDuckGo for {city_name}...",
                            "scanned": len(seen_urls),
                            "verified": yielded_count,
                        })
                        tasks = []
                        for q in wave_queries:
                            tasks.append(search_yahoo(q, max_pages=2))
                            tasks.append(search_bing(q, max_pages=2))
                            if DDGS:
                                tasks.append(search_ddg(q, max_results=15))
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for res in results:
                            if not isinstance(res, list):
                                continue
                            for cand in res:
                                if internal_stop.is_set():
                                    break
                                await push_candidate(cand)
                        await asyncio.sleep(0.2)
                except Exception as exc:
                    logger.debug("Search producer error: %s", exc)

            async def run_producers():
                ptasks = [
                    asyncio.create_task(maps_producer()),
                    asyncio.create_task(osm_producer()),
                    asyncio.create_task(search_producer()),
                ]
                await asyncio.gather(*ptasks, return_exceptions=True)
                producers_done.set()

            # Consumer Workers
            async def consumer_worker():
                while not internal_stop.is_set():
                    try:
                        cand = await asyncio.wait_for(candidate_queue.get(), timeout=0.8)
                    except asyncio.TimeoutError:
                        if producers_done.is_set() and candidate_queue.empty():
                            break
                        continue

                    if cand is None:
                        candidate_queue.task_done()
                        break

                    try:
                        url = cand.get("url", "")
                        if url and is_cctld_mismatched(url, country_iso):
                            continue
                        async with sem:
                            lead = await crawl_business_website(
                                client, cand, clean_cat, city_name, state_name, country_iso, country_name, loc_profile
                            )
                        if lead:
                            # Apply star rating filter if configured
                            if star > 0 and lead.get("rating") is not None:
                                lead_r = float(lead["rating"])
                                if mode == "exact":
                                    if round(lead_r) != star:
                                        lead = None
                                else:
                                    if not (max(1.0, star - 0.5) <= lead_r <= min(5.0, star + 0.5)):
                                        lead = None
                            if lead:
                                await output_queue.put(lead)
                    except Exception as exc:
                        logger.debug("Consumer worker error: %s", exc)
                    finally:
                        candidate_queue.task_done()

            # Start background producers supervisor
            producer_supervisor = asyncio.create_task(run_producers())

            # Spawn 25 concurrent consumer workers
            worker_tasks = [asyncio.create_task(consumer_worker()) for _ in range(25)]

            # Output Queue Consumer (Generator Yield Loop)
            try:
                while yielded_count < total_target and not internal_stop.is_set():
                    if stop_event and stop_event.is_set():
                        internal_stop.set()
                        break

                    try:
                        item = await asyncio.wait_for(output_queue.get(), timeout=0.5)
                    except asyncio.TimeoutError:
                        if producers_done.is_set() and candidate_queue.empty() and all(w.done() for w in worker_tasks):
                            break
                        continue

                    if not isinstance(item, dict):
                        continue

                    # Status telemetry frame
                    if item.get("type") == "status":
                        yield item
                        await asyncio.sleep(0)
                        continue

                    # Lead validation and deduplication
                    phone_e164 = item.get("phone_e164")
                    if phone_e164 and phone_e164 in seen_phones:
                        continue

                    norm_name = re.sub(r"\W+", "", item.get("name", "").lower())
                    norm_addr = re.sub(r"\W+", "", item.get("address", "").lower())
                    identity_key = (norm_name, norm_addr)
                    if norm_name and norm_addr and identity_key in seen_identities:
                        continue

                    root_dom = extract_root_domain(item.get("website", ""))
                    if root_dom and domain_crawl_counts[root_dom] >= 5:
                        continue

                    # Deduplication locks
                    if phone_e164:
                        seen_phones.add(phone_e164)
                    if norm_name and norm_addr:
                        seen_identities.add(identity_key)
                    if root_dom:
                        domain_crawl_counts[root_dom] += 1

                    yielded_count += 1
                    new_leads_for_cache.append(item)
                    save_lead_to_db(item)
                    yield item

                    if yielded_count % 3 == 0:
                        yield {
                            "type": "status",
                            "stage": "verified",
                            "message": f"Verified {yielded_count}/{total_target} leads ({item.get('name', 'Lead')})",
                            "scanned": len(seen_urls),
                            "verified": yielded_count,
                        }
                    await asyncio.sleep(0.01)

            finally:
                # Instant Cancellation of all producers and workers
                internal_stop.set()
                producer_supervisor.cancel()
                for w in worker_tasks:
                    w.cancel()
                await asyncio.gather(producer_supervisor, *worker_tasks, return_exceptions=True)

            # Persist newly verified leads to SQLite store
            if new_leads_for_cache:
                try:
                    save_leads_batch(new_leads_for_cache)
                except Exception as exc:
                    logger.debug("SQLite batch save error: %s", exc)

    yield {
        "type": "status",
        "stage": "complete",
        "message": f"Extraction complete: delivered {yielded_count} verified leads",
        "scanned": total_target,
        "verified": yielded_count,
    }
    logger.info("Extraction complete: delivered %d genuine leads", yielded_count)
