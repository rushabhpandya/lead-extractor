"""
services/enrichment.py — Global phone validation (E.164), email ranking, Instagram crawling.
Replaces the India-only TRAI gatekeeper with the `phonenumbers` library for global support.
"""

from __future__ import annotations

import asyncio
import re
import logging
from typing import Optional, List, Tuple, Dict
from urllib.parse import urlparse, unquote

try:
    import phonenumbers
    from phonenumbers import PhoneNumberType, NumberParseException
    _HAS_PHONENUMBERS = True
except ImportError:
    _HAS_PHONENUMBERS = False

try:
    import dns.asyncresolver
    import dns.resolver
    _HAS_DNS = True
except ImportError:
    _HAS_DNS = False

logger = logging.getLogger("enrichment")

# ---------------------------------------------------------------------------
# Sovereign ccTLD & Language/Script Geo Alignment (Upgrade 1)
# ---------------------------------------------------------------------------

# Generic & Commercial TLDs that are globally used and exempt from ccTLD mismatching
GENERIC_COMMERCIAL_TLDS: Set[str] = {
    "com", "net", "org", "edu", "gov", "mil", "int",
    "io", "co", "ai", "me", "tv", "biz", "info", "studio", "design", "agency",
    "shop", "store", "art", "gallery", "app", "dev", "online", "site", "world",
    "global", "pro", "xyz", "cloud", "tech", "group", "ltd", "club", "space",
    "live", "life", "center", "expert", "cc", "mobi", "link", "media", "top",
    "vip", "work", "fit", "run", "guru", "solutions", "boutique", "fashion",
    "luxury", "house", "style", "direct", "city", "zone", "today", "show",
}

# Two-letter sovereign ccTLD mapping to ISO-3166 alpha-2 country codes
CCTLD_TO_ISO: Dict[str, str] = {
    "uk": "GB", "de": "DE", "fr": "FR", "it": "IT", "es": "ES",
    "in": "IN", "ca": "CA", "au": "AU", "jp": "JP", "ae": "AE",
    "sg": "SG", "nz": "NZ", "ch": "CH", "at": "AT", "nl": "NL",
    "be": "BE", "se": "SE", "no": "NO", "dk": "DK", "fi": "FI",
    "pl": "PL", "pt": "PT", "ie": "IE", "za": "ZA", "br": "BR",
    "mx": "MX", "us": "US", "cn": "CN", "hk": "HK", "tw": "TW",
    "kr": "KR", "cz": "CZ", "gr": "GR", "hu": "HU", "ro": "RO",
    "bg": "BG", "hr": "HR", "si": "SI", "sk": "SK", "ru": "RU",
    "tr": "TR", "il": "IL", "sa": "SA", "cl": "CL", "ar": "AR",
}

# Dominant script patterns for geo validation
_JAPANESE_RE = re.compile(r'[\u3040-\u30ff]')
_CHINESE_RE = re.compile(r'[\u4e00-\u9fff]')
_CYRILLIC_RE = re.compile(r'[\u0400-\u04ff]')
_ARABIC_RE = re.compile(r'[\u0600-\u06ff]')
_DEVANAGARI_RE = re.compile(r'[\u0900-\u097f]')


def extract_domain_cctld(domain_or_url: str) -> Optional[str]:
    """
    Extract sovereign 2-letter ccTLD mapped to ISO-3166 alpha-2 country code.
    Returns None if domain uses a generic or commercial TLD (.com, .net, .io, .co, .ai, etc.).
    """
    if not domain_or_url:
        return None
    raw = domain_or_url.strip()
    if "@" in raw:
        raw = raw.split("@")[-1].strip()
    if "://" in raw:
        raw = urlparse(raw).netloc
    raw = re.sub(r":\d+$", "", raw).lower().strip()
    if raw.startswith("www."):
        raw = raw[4:]

    parts = raw.split(".")
    if len(parts) < 2:
        return None

    last_part = parts[-1].lower()

    # If the TLD is in our generic/commercial exempt list, it's not a sovereign ccTLD
    if last_part in GENERIC_COMMERCIAL_TLDS:
        return None

    # Handle multi-part public suffixes like .co.uk, .com.au, .co.in
    if len(parts) >= 3 and parts[-2].lower() in ("co", "com", "org", "net", "ltd", "firm", "gen"):
        if last_part in CCTLD_TO_ISO:
            return CCTLD_TO_ISO[last_part]
        if len(last_part) == 2:
            return last_part.upper()

    # Standard two-letter ccTLD check
    if len(last_part) == 2:
        if last_part in CCTLD_TO_ISO:
            return CCTLD_TO_ISO[last_part]
        return last_part.upper()

    return None


def is_cctld_mismatched(domain_or_url_or_email: str, target_iso: Optional[str]) -> bool:
    """
    Sovereign ccTLD Gatekeeper:
    Checks if a website or extracted email uses a sovereign 2-letter country-code TLD
    that blatantly contradicts target_iso.
    Returns True if MISMATCHED (should be rejected), False otherwise.
    """
    if not domain_or_url_or_email or not target_iso:
        return False
    target = target_iso.upper().strip()
    cctld_iso = extract_domain_cctld(domain_or_url_or_email)
    if not cctld_iso:
        return False  # Generic or international domain

    # UK and GB alias
    if (cctld_iso == "GB" and target == "UK") or (cctld_iso == "UK" and target == "GB"):
        return False

    return cctld_iso != target


def is_script_or_lang_mismatched(html: str, target_iso: Optional[str]) -> bool:
    """
    Address & Script Consistency:
    Detects if dominant Unicode script or <html lang="..."> blatantly contradicts
    the target country's official languages/scripts.
    """
    if not html or not target_iso:
        return False
    target = target_iso.upper().strip()

    # 1. Inspect <html lang="..."> tag
    m_lang = re.search(r'<html[^>]+lang=["\']([a-zA-Z]{2}(?:-[a-zA-Z0-9]+)?)["\']', html[:4000], re.I)
    if m_lang:
        lang_code = m_lang.group(1).lower().split("-")[0]
        western_targets = {"US", "GB", "CA", "AU", "NZ", "IE", "FR", "IT", "DE", "ES", "NL", "CH", "AT"}
        if target in western_targets and lang_code in ("ja", "zh", "ru", "ko", "ar", "hi"):
            return True

    # 2. Inspect text content for dominant non-target scripts
    clean_sample = re.sub(r'<[^>]+>', ' ', html[:40000])
    western_targets = {"US", "GB", "CA", "AU", "NZ", "IE", "FR", "IT", "DE", "ES", "NL", "CH", "AT", "SE", "NO", "DK", "FI"}
    if target in western_targets:
        if len(_JAPANESE_RE.findall(clean_sample)) >= 10:
            return True
        if len(_CHINESE_RE.findall(clean_sample)) >= 20:
            return True
        if len(_CYRILLIC_RE.findall(clean_sample)) >= 15:
            return True
        if len(_ARABIC_RE.findall(clean_sample)) >= 15:
            return True

    return False


# ---------------------------------------------------------------------------
# Phone Validation & E.164 Formatting
# ---------------------------------------------------------------------------

# India TRAI fallback (when phonenumbers not available)
_INDIA_MOBILE_RE = re.compile(r'^(?:[789]\d{9}|6(?:0[0-9]\d{7}|[23]\d{8}))$')

# Strict RFC email regex
STRICT_EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,12}$')

# Sequential and repetitive sequences for entropy rejection
ASC_SEQS = ("012345", "123456", "234567", "345678", "456789", "567890")
DESC_SEQS = ("987654", "876543", "765432", "654321", "543210")


def is_dummy_or_low_entropy_phone(nsn: str, country_code: int = 0) -> bool:
    """
    Check if a national significant number (digits only) is a placeholder, dummy,
    or fictional template number.
    """
    digits = re.sub(r"\D", "", nsn)
    if len(digits) < 6:
        return True

    # 1. 6 or more identical consecutive digits (e.g. 000000, 999999, 111111)
    if re.search(r"(\d)\1{5,}", digits):
        return True

    # 2. Ascending or descending sequential runs of 6 or more digits
    if any(seq in digits for seq in ASC_SEQS) or any(seq in digits for seq in DESC_SEQS):
        return True

    # 3. North American (NANP, country_code=1) 555 fictional exchanges
    if country_code == 1:
        if len(digits) == 10 and digits[3:6] == "555":
            return True
        elif len(digits) == 11 and digits.startswith("1") and digits[4:7] == "555":
            return True
        elif digits.startswith("555"):
            return True

    return False

# Raw phone extraction pattern — matches international and local formats
_PHONE_EXTRACT_RE = re.compile(
    r'(?<!\d)(?:\+?[1-9]\d{0,2}[\s\-.]?)?'
    r'(?:\(?\d{2,4}\)?[\s\-.]?)'
    r'\d{2,4}[\s\-.]?\d{2,4}[\s\-.]?\d{0,4}'
    r'(?!\d)',
    re.MULTILINE,
)

# Common noise numbers to reject
_NOISE_PHONES = {
    "1800", "0000000000", "1234567890", "9999999999",
    "8888888888", "7777777777", "0123456789",
}


def extract_phones_from_text(text: str, default_region: str = "IN") -> List[str]:
    """
    Extract all candidate phone numbers from raw text.
    Returns cleaned raw strings (not yet E.164 validated).
    """
    candidates = []
    seen = set()

    if _HAS_PHONENUMBERS:
        for match in phonenumbers.PhoneNumberMatcher(text, default_region):
            raw = phonenumbers.format_number(
                match.number, phonenumbers.PhoneNumberFormat.E164
            )
            if raw not in seen and raw not in _NOISE_PHONES:
                seen.add(raw)
                candidates.append(raw)
    else:
        # Fallback regex
        for m in _PHONE_EXTRACT_RE.finditer(text):
            raw = re.sub(r"[\s\-.()\u00a0]", "", m.group())
            raw = raw.lstrip("0")
            if 7 <= len(raw) <= 15 and raw not in seen and raw not in _NOISE_PHONES:
                seen.add(raw)
                candidates.append(raw)

    return candidates


def validate_and_format_phone(
    raw_phone: str,
    default_region: str = "IN",
    target_iso: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], bool, Optional[str]]:
    """
    Validate a raw phone string and return (e164, display, is_whatsapp_ready, verified_region).

    Strict Target-Country Phone Region Alignment (Rule 1):
    If target_iso (or default_region) is provided, immediately discards candidate
    numbers belonging to foreign calling codes or telecom regions.

    Returns (None, None, False, None) if invalid or mismatched.
    """
    if not raw_phone:
        return None, None, False, None

    cleaned = re.sub(r"[\s\-.()\u00a0]", "", raw_phone)
    if not cleaned:
        return None, None, False, None

    # Ensure leading + for international format
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]

    target_country = (target_iso or default_region or "").upper().strip()

    if _HAS_PHONENUMBERS:
        target_cc = phonenumbers.country_code_for_region(target_country) if target_country else 0

        for attempt in [cleaned, cleaned.replace("+", "")]:
            try:
                parsed = phonenumbers.parse(attempt, target_country or default_region)
                if phonenumbers.is_valid_number(parsed):
                    nsn = str(parsed.national_number)
                    if is_dummy_or_low_entropy_phone(nsn, parsed.country_code):
                        continue

                    # Strict target-country alignment (Rule 1)
                    if target_cc > 0:
                        num_region = phonenumbers.region_code_for_number(parsed)
                        calling_match = (parsed.country_code == target_cc)
                        region_match = (num_region == target_country)
                        
                        if not (calling_match or region_match):
                            continue

                        # In non-NANP countries (+1), if a distinct foreign region is detected, reject
                        if target_cc != 1 and num_region and num_region != target_country:
                            valid_regions = phonenumbers.region_codes_for_country_code(target_cc)
                            if target_country not in valid_regions:
                                continue

                    e164 = phonenumbers.format_number(
                        parsed, phonenumbers.PhoneNumberFormat.E164
                    )
                    display = phonenumbers.format_number(
                        parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL
                    )
                    num_type = phonenumbers.number_type(parsed)
                    # Strict Phone Type Classification for WhatsApp (Fix 3):
                    # Only MOBILE or FIXED_LINE_OR_MOBILE.
                    # Explicitly excludes TOLL_FREE, FIXED_LINE, VOIP, SHARED_COST, PREMIUM_RATE, UAN.
                    whatsapp = num_type in (
                        PhoneNumberType.MOBILE,
                        PhoneNumberType.FIXED_LINE_OR_MOBILE,
                    ) and num_type not in (
                        PhoneNumberType.TOLL_FREE,
                        PhoneNumberType.FIXED_LINE,
                        PhoneNumberType.VOIP,
                        PhoneNumberType.SHARED_COST,
                        PhoneNumberType.PREMIUM_RATE,
                        PhoneNumberType.UAN,
                        PhoneNumberType.PAGER,
                    )
                    verified_region = phonenumbers.region_code_for_number(parsed) or target_country
                    return e164, display, whatsapp, verified_region
            except (NumberParseException, Exception):
                continue
        return None, None, False, None

    # Fallback: India only
    digits = re.sub(r"\D", "", cleaned)
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    if _INDIA_MOBILE_RE.match(digits) and not is_dummy_or_low_entropy_phone(digits, 91):
        display = f"+91 {digits[:5]} {digits[5:]}"
        return f"+91{digits}", display, True, "IN"
    return None, None, False, None


def best_phone(
    phones: List[str],
    default_region: str = "IN",
    target_iso: Optional[str] = None,
) -> Tuple[str, Optional[str], bool, Optional[str]]:
    """
    Pick the best phone from a list. Returns (display/raw, e164, is_whatsapp_ready, verified_region).
    Prefers mobile numbers; falls back to first valid.
    """
    mobile_candidate = None
    first_valid = None

    for raw in phones:
        e164, display, wa, v_reg = validate_and_format_phone(raw, default_region, target_iso)
        if e164 is None:
            continue
        if first_valid is None:
            first_valid = (display or raw, e164, wa, v_reg)
        if wa and mobile_candidate is None:
            mobile_candidate = (display or raw, e164, wa, v_reg)

    if mobile_candidate:
        return mobile_candidate
    if first_valid:
        return first_valid
    return (phones[0] if phones else "", None, False, None)


# ---------------------------------------------------------------------------
# Email Ranking
# ---------------------------------------------------------------------------

# Priority prefixes — higher index = lower priority
_EMAIL_PRIORITY = [
    "founder", "director", "gallery", "studio", "owner", "partner",
    "sales", "enquiries", "enquiry", "contact", "hello", "info",
    "admin", "mail", "team",
]


def clean_and_validate_email(raw_email: str) -> Optional[str]:
    """
    URL-decode, sanitize, and strictly validate email against RFC syntax.
    Strips URL-encoded prefixes (%20, %0A), mailto: prefixes, and parameters.
    """
    if not raw_email:
        return None
    unquoted = unquote(raw_email).strip()
    if unquoted.lower().startswith("mailto:"):
        unquoted = unquoted[7:].strip()
    unquoted = re.split(r"[?&]", unquoted)[0].strip()
    unquoted = unquoted.strip(" \t\n\r'\"<>(),;:")
    if not STRICT_EMAIL_RE.match(unquoted):
        return None
    if re.search(r"\.(png|jpg|jpeg|gif|svg|webp|css|js|woff|woff2)$", unquoted, re.I):
        return None
    if any(ign in unquoted.lower() for ign in ["sentry", "cloudflare", "wixpress", "example.com", "domain.com", "email.com"]):
        return None
    return unquoted.lower()


def rank_emails(emails: List[str]) -> List[str]:
    """Sanitize, validate, and return emails sorted by business-relevance priority."""
    valid_cleaned: List[str] = []
    seen = set()
    for em in emails:
        clean = clean_and_validate_email(em)
        if clean and clean not in seen:
            seen.add(clean)
            valid_cleaned.append(clean)

    def _score(email: str) -> int:
        local = email.split("@")[0].lower()
        for idx, prefix in enumerate(_EMAIL_PRIORITY):
            if prefix in local:
                return idx
        return len(_EMAIL_PRIORITY)

    return sorted(valid_cleaned, key=_score)


# ---------------------------------------------------------------------------
# Instagram Extraction
# ---------------------------------------------------------------------------

_INSTAGRAM_RE = re.compile(
    r'https?://(?:www\.)?instagram\.com/([A-Za-z0-9_.]{1,30})/?',
    re.IGNORECASE,
)
_IG_SKIP_PATHS = {
    "p", "reel", "reels", "explore", "stories", "accounts",
    "direct", "tv", "ar", "hashtag", "shoppingtag", "about",
    "developer", "legal", "privacy", "help",
}


def extract_instagram(html: str, page_url: str = "") -> Tuple[Optional[str], Optional[str]]:
    """
    Extract the best Instagram @handle from page HTML.
    Returns (handle, full_url) or (None, None).
    """
    matches = _INSTAGRAM_RE.findall(html)
    for handle in matches:
        if handle.lower() in _IG_SKIP_PATHS:
            continue
        if len(handle) < 2:
            continue
        return handle, f"https://www.instagram.com/{handle}/"
    return None, None


# ---------------------------------------------------------------------------
# DNS MX Deliverability Verification (Upgrade 3)
# ---------------------------------------------------------------------------

mx_cache: Dict[str, bool] = {}


async def verify_email_mx(email: str) -> bool:
    """
    Asynchronously verify MX records for the given email domain.
    Uses dns.asyncresolver.Resolver with a 1.5s timeout and in-memory cache.
    Fails open safely (returns False without raising exceptions).
    """
    if not email or "@" not in email:
        return False
    domain = email.split("@")[1].strip().lower()
    if not domain or "." not in domain:
        return False

    if domain in mx_cache:
        return mx_cache[domain]

    if not _HAS_DNS:
        return False

    try:
        resolver = dns.asyncresolver.Resolver()
        resolver.lifetime = 1.5
        resolver.timeout = 1.5
        answers = await resolver.resolve(domain, "MX")
        has_mx = len(answers) > 0
        mx_cache[domain] = has_mx
        return has_mx
    except Exception as exc:
        logger.debug("MX verification failed for domain %s: %s", domain, exc)
        mx_cache[domain] = False
        return False


async def rank_and_verify_emails(raw_emails: List[str]) -> Tuple[str, bool, List[str]]:
    """
    Sanitize and rank candidate emails, performing async MX verification.
    Prefers MX-verified emails, and ranks direct/decision-maker prefixes ahead of generic ones.
    Returns (best_email, is_mx_verified, ranked_emails_list).
    """
    sanitized: List[str] = []
    seen = set()
    for em in raw_emails:
        clean = clean_and_validate_email(em)
        if clean and clean not in seen:
            seen.add(clean)
            sanitized.append(clean)

    if not sanitized:
        return "", False, []

    # MX check tasks for all unique candidate domains
    mx_tasks = [verify_email_mx(em) for em in sanitized]
    mx_results = await asyncio.gather(*mx_tasks, return_exceptions=True)

    email_mx_map: Dict[str, bool] = {}
    for em, res in zip(sanitized, mx_results):
        email_mx_map[em] = bool(res) if isinstance(res, bool) else False

    def _sort_key(email: str) -> Tuple[int, int]:
        is_mx = 0 if email_mx_map.get(email, False) else 1  # 0 is better (True first)
        local = email.split("@")[0].lower()
        prefix_score = len(_EMAIL_PRIORITY)
        for idx, prefix in enumerate(_EMAIL_PRIORITY):
            if prefix in local:
                prefix_score = idx
                break
        return (is_mx, prefix_score)

    ranked = sorted(sanitized, key=_sort_key)
    best_email = ranked[0]
    best_mx = email_mx_map.get(best_email, False)
    return best_email, best_mx, ranked


# ---------------------------------------------------------------------------
# Zero-Request Marketing Signal Fingerprinting (Upgrade 2)
# ---------------------------------------------------------------------------

_META_PIXEL_RE = re.compile(
    r'(?:fbq\s*\(|connect\.facebook\.net/[a-zA-Z_]+/fbevents\.js|facebook\.com/tr\?id=|_fbq)',
    re.IGNORECASE,
)

_GOOGLE_ADS_RE = re.compile(
    r'(?:googletagmanager\.com/gtag|googleadservices\.com|AW-\d+|gtm\.js|adsbygoogle)',
    re.IGNORECASE,
)

_STRICT_CMS_SIGNATURES = [
    ("Shopify", re.compile(r'(?:cdn\.shopify\.com|Shopify\.theme|myshopify\.com)', re.I)),
    ("WooCommerce", re.compile(r'(?:/plugins/woocommerce/|woocommerce-js|wc-block)', re.I)),
    ("WordPress", re.compile(r'(?:/wp-content/|/wp-includes/|<meta[^>]+name=["\']generator["\'][^>]+content=["\'][^"\']*WordPress)', re.I)),
    ("Webflow", re.compile(r'(?:assets\.website-files\.com|assets-global\.website-files\.com|data-wf-site)', re.I)),
    ("Squarespace", re.compile(r'(?:static1\.squarespace\.com|squarespace-cdn\.com|Static\.SQUARESPACE_CONTEXT)', re.I)),
    ("Wix", re.compile(r'(?:static\.wixstatic\.com|wix-code-sdk|X-Wix-)', re.I)),
    ("Magento", re.compile(r'(?:text/x-magento-init|Mage\.Cookies|/static/frontend/Magento/|(?:requirejs-config\.js.*mage/|mage/.*requirejs-config\.js))', re.I)),
    ("Ghost", re.compile(r'(?:<meta[^>]+name=["\']generator["\'][^>]+content=["\'][^"\']*Ghost|ghost-sdk)', re.I)),
    ("Framer", re.compile(r'framerusercontent\.com', re.I)),
    ("Next.js", re.compile(r'/_next/static/', re.I)),
]

_WHATSAPP_LINK_RE = re.compile(
    r'https?://(?:wa\.me|api\.whatsapp\.com/send\??)[^\s"\'<>]+',
    re.IGNORECASE,
)

_LINKEDIN_COMPANY_RE = re.compile(
    r'https?://(?:www\.)?linkedin\.com/company/([A-Za-z0-9_\-]+)/?',
    re.IGNORECASE,
)


def detect_meta_pixel(html: str) -> bool:
    """Detect Meta/Facebook Pixel scripts in page HTML."""
    if not html:
        return False
    return bool(_META_PIXEL_RE.search(html))


def detect_google_ads(html: str) -> bool:
    """Detect Google Ads / Tag Manager / conversion tags in page HTML."""
    if not html:
        return False
    return bool(_GOOGLE_ADS_RE.search(html))


def detect_cms_platform(html: str) -> str:
    """
    Detect primary web CMS/eCommerce platform from structural DOM & asset signatures.
    Returns CMS name or empty string "" (no noisy badge) when no explicit signature matches.
    """
    if not html:
        return ""
    for name, pattern in _STRICT_CMS_SIGNATURES:
        if pattern.search(html):
            return name
    return ""


def extract_whatsapp_url(
    html: str,
    phone_e164: Optional[str] = None,
    is_whatsapp_ready: bool = False,
) -> str:
    """
    Extract direct WhatsApp link from HTML or construct wa.me URL ONLY from verified mobile phone.
    Excludes toll-free and fixed-line numbers.
    """
    if html:
        m = _WHATSAPP_LINK_RE.search(html)
        if m:
            clean_url = m.group(0).rstrip(".,;')\"")
            return clean_url

    if is_whatsapp_ready and phone_e164:
        digits = re.sub(r"\D", "", phone_e164)
        if len(digits) >= 8:
            return f"https://wa.me/{digits}"

    return ""


def extract_linkedin_url(html: str) -> str:
    """Extract official company LinkedIn URL if present in HTML."""
    if not html:
        return ""
    m = _LINKEDIN_COMPANY_RE.search(html)
    if m:
        company_slug = m.group(1).strip()
        if company_slug and company_slug.lower() not in ("company", "share", "home"):
            return f"https://www.linkedin.com/company/{company_slug}/"
    return ""


def calculate_lead_score(
    has_phone: bool,
    has_whatsapp: bool,
    email: str,
    email_mx_verified: bool,
    has_instagram: bool,
    address: str,
    city: str,
    country_name: str,
    has_meta_pixel: bool,
    has_google_ads: bool,
) -> int:
    """
    Compute a deterministic outreach-readiness score (0–100):
      - Valid callable phone (+20), WhatsApp-ready/direct WA (+10)
      - MX-verified email (+25), plus bonus (+10) if non-role/founder-style prefix
      - Active Instagram handle (+15)
      - Street-level physical address (+10)
      - Active ad tracking (has_meta_pixel or has_google_ads) (+10)
    """
    score = 0

    # 1. Phone (+20)
    if has_phone:
        score += 20

    # 2. WhatsApp (+10)
    if has_whatsapp:
        score += 10

    # 3. Email (+25 for MX, +10 bonus for founder/direct prefix)
    if email and email_mx_verified:
        score += 25
        local_part = email.split("@")[0].lower()
        generic_roles = {
            "info", "sales", "contact", "support", "admin", "office",
            "help", "enquiries", "enquiry", "hello", "mail", "team",
            "service", "services", "billing", "general", "inquiry", "inquiries",
        }
        if local_part not in generic_roles and not any(
            local_part.startswith(g + ".") or local_part.startswith(g + "_") for g in generic_roles
        ):
            score += 10

    # 4. Instagram (+15)
    if has_instagram:
        score += 15

    # 5. Street-level physical address (+10)
    if address:
        clean_addr = address.strip().lower()
        fallback_geo = f"{city}, {country_name}".strip().lower()
        # Has street details beyond the generic city/country string
        if clean_addr != fallback_geo and len(clean_addr) > len(fallback_geo):
            score += 10

    # 6. Active ad tracking (+10)
    if has_meta_pixel or has_google_ads:
        score += 10

    return max(0, min(100, score))
