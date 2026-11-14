"""
services/enrichment.py — Global phone validation (E.164), email ranking, Instagram crawling.
Replaces the India-only TRAI gatekeeper with the `phonenumbers` library for global support.
"""

from __future__ import annotations

import re
import logging
from typing import Optional, List, Tuple
from urllib.parse import urlparse, unquote

try:
    import phonenumbers
    from phonenumbers import PhoneNumberType, NumberParseException
    _HAS_PHONENUMBERS = True
except ImportError:
    _HAS_PHONENUMBERS = False

logger = logging.getLogger("enrichment")

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
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Validate a raw phone string and return (e164, display, is_whatsapp_ready).

    is_whatsapp_ready = True when the number is a mobile or fixed_or_mobile type.

    Returns (None, None, False) if invalid.
    """
    if not raw_phone:
        return None, None, False

    cleaned = re.sub(r"[\s\-.()\u00a0]", "", raw_phone)
    if not cleaned:
        return None, None, False

    # Ensure leading + for E.164 candidates
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]
    elif not cleaned.startswith("+") and len(cleaned) >= 10:
        # Try with + prefix
        cleaned_with_plus = "+" + cleaned

    if _HAS_PHONENUMBERS:
        for attempt in [cleaned, cleaned.replace("+", "")]:
            try:
                parsed = phonenumbers.parse(attempt, default_region)
                if phonenumbers.is_valid_number(parsed):
                    nsn = str(parsed.national_number)
                    if is_dummy_or_low_entropy_phone(nsn, parsed.country_code):
                        continue
                    e164 = phonenumbers.format_number(
                        parsed, phonenumbers.PhoneNumberFormat.E164
                    )
                    display = phonenumbers.format_number(
                        parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL
                    )
                    num_type = phonenumbers.number_type(parsed)
                    whatsapp = num_type in (
                        PhoneNumberType.MOBILE,
                        PhoneNumberType.FIXED_LINE_OR_MOBILE,
                        PhoneNumberType.FIXED_LINE,
                    )
                    return e164, display, whatsapp
            except (NumberParseException, Exception):
                continue
        return None, None, False

    # Fallback: India only
    digits = re.sub(r"\D", "", cleaned)
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    if _INDIA_MOBILE_RE.match(digits) and not is_dummy_or_low_entropy_phone(digits, 91):
        display = f"+91 {digits[:5]} {digits[5:]}"
        return f"+91{digits}", display, True
    return None, None, False


def best_phone(
    phones: List[str],
    default_region: str = "IN",
) -> Tuple[str, Optional[str], bool]:
    """
    Pick the best phone from a list. Returns (raw, e164, is_whatsapp_ready).
    Prefers mobile numbers; falls back to first valid.
    """
    mobile_candidate = None
    first_valid = None

    for raw in phones:
        e164, display, wa = validate_and_format_phone(raw, default_region)
        if e164 is None:
            continue
        if first_valid is None:
            first_valid = (display or raw, e164, wa)
        if wa and mobile_candidate is None:
            mobile_candidate = (display or raw, e164, wa)

    if mobile_candidate:
        return mobile_candidate
    if first_valid:
        return first_valid
    return (phones[0] if phones else "", None, False)


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
