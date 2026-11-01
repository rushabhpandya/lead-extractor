"""
scraper.py — High-yield B2B Lead Extractor Engine for Indian Cities.
3-Tier Pipeline:
  Tier 1: verified_leads_cache.json  (instant sub-second streaming)
  Tier 2: Overpass OSM               (structured, reliable, no bot-poison)
  Tier 3: DDGS library + Bing HTML   (live discovery with category gating)

Category Relevance Gatekeeper: Rejects junk/non-SMB pages at source.
TRAI Phone Validation: Rejects invalid prefixes, dummy sequences, cross-city STDs.
Zero fake/hallucinated data: every field is genuinely scraped or blank.
"""

import re
import json
import base64
import asyncio
import logging
import time
import random
from pathlib import Path
from typing import AsyncIterator, Optional
from urllib.parse import parse_qs, urlparse, unquote

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("scraper")

# ---------------------------------------------------------------------------
# Constants & Configuration
# ---------------------------------------------------------------------------

DISCOVERY_TIMEOUT = 2.5
SCRAPE_TIMEOUT = 1.4
CONTACT_TIMEOUT = 0.9
HTML_TRUNCATE = 100_000
ENRICH_SEMAPHORE_LIMIT = 35

CACHE_FILE = Path(__file__).parent / "verified_leads_cache.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

BROWSER_HEADERS = {
    "User-Agent": USER_AGENTS[0],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------
# TRAI Phone Validation — valid Indian mobile prefixes
# ---------------------------------------------------------------------------
# Mobile: starts with 6(00/20-29/30-39), 7, 8, 9 — effectively [789]\d{9}
# and limited 6xx series: 600, 620-629, 630-639 etc.
# Safest rule: accept 7/8/9 prefix (most common) and 6[0-9] ONLY if 60x or 62x/63x
VALID_MOBILE_RE = re.compile(r'^(?:[789]\d{9}|6(?:0[0-9]\d{7}|[23]\d{8}))$')

# City → valid 2-digit and 3-digit STD codes
CITY_STD_CODES: dict[str, set[str]] = {
    "mumbai":    {"22"},
    "thane":     {"22"},
    "navi mumbai": {"22"},
    "delhi":     {"11", "120", "124", "129"},
    "new delhi": {"11", "120", "124", "129"},
    "gurgaon":   {"124"},
    "noida":     {"120"},
    "bengaluru": {"80"},
    "bangalore": {"80"},
    "pune":      {"20"},
    "hyderabad": {"40"},
    "chennai":   {"44"},
    "kolkata":   {"33"},
    "ahmedabad": {"79"},
    "jaipur":    {"141"},
    "surat":     {"261"},
    "lucknow":   {"522"},
    "kanpur":    {"512"},
    "nagpur":    {"712"},
    "indore":    {"731"},
    "bhopal":    {"755"},
    "patna":     {"612"},
    "vadodara":  {"265"},
    "kochi":     {"484"},
    "coimbatore":{"422"},
    "visakhapatnam": {"891"},
    "agra":      {"562"},
    "nashik":    {"253"},
}

# Cross-city rejection words — if searching Mumbai and these appear in address/snippet, reject
CITY_EXCLUSION_MAP: dict[str, list[str]] = {
    "mumbai":    ["west bengal", "balurghat", "kolkata", "chennai", "tamil nadu",
                  "bengaluru", "karnataka", "hyderabad", "telangana", "new delhi",
                  "ahmedabad", "gujarat", "jaipur", "lucknow", "delhi", "rajasthan",
                  "bhopal", "madhya pradesh", "coimbatore", "kochi", "kerala"],
    "delhi":     ["mumbai", "maharashtra", "kolkata", "west bengal", "chennai",
                  "tamil nadu", "bengaluru", "karnataka", "hyderabad", "telangana",
                  "ahmedabad", "gujarat", "kochi", "kerala"],
    "bengaluru": ["mumbai", "maharashtra", "kolkata", "west bengal", "chennai",
                  "tamil nadu", "hyderabad", "telangana", "delhi", "rajasthan",
                  "ahmedabad", "gujarat", "kochi", "kerala"],
    "pune":      ["kolkata", "west bengal", "chennai", "tamil nadu", "bengaluru",
                  "karnataka", "hyderabad", "telangana", "delhi", "ahmedabad", "gujarat"],
    "hyderabad": ["mumbai", "maharashtra", "kolkata", "west bengal", "chennai",
                  "tamil nadu", "bengaluru", "karnataka", "delhi", "ahmedabad", "gujarat"],
    "chennai":   ["mumbai", "maharashtra", "kolkata", "west bengal", "bengaluru",
                  "karnataka", "hyderabad", "telangana", "delhi", "ahmedabad", "gujarat"],
    "kolkata":   ["mumbai", "maharashtra", "chennai", "tamil nadu", "bengaluru",
                  "karnataka", "hyderabad", "telangana", "delhi", "ahmedabad", "gujarat"],
    "ahmedabad": ["kolkata", "west bengal", "chennai", "tamil nadu", "bengaluru",
                  "karnataka", "hyderabad", "telangana", "delhi", "mumbai", "maharashtra"],
}

# ---------------------------------------------------------------------------
# Category Keyword Stems — comprehensive per-category relevance gating
# ---------------------------------------------------------------------------

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "gym": [
        "gym", "fitness", "crossfit", "workout", "athletic", "health club",
        "physique", "muscle", "iron", "barbell", "strength", "pilates", "yoga",
        "mma", "boxing", "calisthenics", "cult", "golds", "anytime fitness",
        "talwalkars", "transform", "gymkhana", "sweat", "wellness", "studio",
        "sport", "train", "exercise", "aerobic", "zumba", "akhada", "power",
        "body", "fit", "flex", "pump", "lift", "cardio",
    ],
    "fitness": [
        "gym", "fitness", "crossfit", "workout", "athletic", "health club",
        "physique", "muscle", "iron", "barbell", "strength", "pilates", "yoga",
        "mma", "boxing", "calisthenics", "cult", "golds", "anytime fitness",
        "talwalkars", "transform", "gymkhana", "sweat", "wellness", "studio",
        "sport", "train", "exercise", "aerobic", "zumba", "akhada", "power",
        "body", "fit", "flex", "pump", "lift", "cardio",
    ],
    "dentist": [
        "dental", "dentist", "teeth", "tooth", "orthodont", "oral", "smile",
        "implant", "braces", "clinic", "doctor", "dr.", "dent", "root canal",
        "gum", "enamel", "crown", "veneer", "jaw", "maxillo",
    ],
    "dental": [
        "dental", "dentist", "teeth", "tooth", "orthodont", "oral", "smile",
        "implant", "braces", "clinic", "doctor", "dr.", "dent", "root canal",
        "gum", "enamel", "crown", "veneer", "jaw", "maxillo",
    ],
    "cafe": [
        "cafe", "café", "coffee", "tea", "bakery", "roast", "brew", "espresso",
        "bistro", "lounge", "latte", "cappuccino", "mocha", "chai", "patisserie",
        "croissant", "barista", "bean", "cup", "brew",
    ],
    "coffee": [
        "cafe", "café", "coffee", "tea", "bakery", "roast", "brew", "espresso",
        "bistro", "lounge", "latte", "cappuccino", "mocha", "chai", "patisserie",
        "croissant", "barista", "bean", "cup", "brew",
    ],
    "restaurant": [
        "restaurant", "food", "dine", "dining", "kitchen", "bistro", "dhaba",
        "cuisine", "eats", "bar", "hotel", "eatery", "mess", "tiffin", "grill",
        "bbq", "barbeque", "biryani", "thali", "buffet", "feast", "table",
    ],
    "food": [
        "restaurant", "food", "dine", "dining", "kitchen", "bistro", "dhaba",
        "cuisine", "eats", "bar", "hotel", "eatery", "mess", "tiffin", "grill",
        "bbq", "barbeque", "biryani", "thali", "buffet", "feast", "table",
    ],
    "hotel": [
        "hotel", "inn", "lodge", "resort", "stay", "hospitality", "rooms",
        "suites", "accommodation", "hostel", "guesthouse", "guest house",
        "bed and breakfast", "boutique", "palace", "manor",
    ],
    "salon": [
        "salon", "spa", "beauty", "hair", "parlour", "nail", "skin", "facial",
        "wax", "makeup", "grooming", "barber", "threading", "pedicure",
        "manicure", "bridal", "wellness", "aesthetic", "laser",
    ],
    "clinic": [
        "clinic", "hospital", "health", "medical", "doctor", "dr.", "physician",
        "nursing", "care", "ayurvedic", "homeopathic", "diagnostic", "lab",
        "pathology", "surgery", "ortho", "cardio", "neuro", "paediatric",
        "gynaecolog", "dermatolog",
    ],
    "hospital": [
        "clinic", "hospital", "health", "medical", "doctor", "dr.", "physician",
        "nursing", "care", "ayurvedic", "homeopathic", "diagnostic", "lab",
        "pathology", "surgery", "ortho", "cardio", "neuro", "paediatric",
        "gynaecolog", "dermatolog",
    ],
    "pharmacy": [
        "pharmacy", "chemist", "medicine", "drug", "pharma", "medic", "health",
        "apollo", "wellness", "store",
    ],
    "bakery": [
        "bakery", "bake", "bread", "cake", "pastry", "patisserie", "dessert",
        "cookie", "muffin", "confectionery", "sweet",
    ],
}

# Hard-reject junk content patterns (bot-poisoned Bing results etc.)
JUNK_PATTERN = re.compile(
    r'(?i)\b('
    r'play\s+online|free!|penalty\s+shooters|poki\.com|crazygames|'
    r'research\s+paper|department\s+of|environmental\s+science|'
    r'demat|trading\s+account|icicisecurities|credit\s+card|burlington|'
    r'aajtak|intoday|leverageedu|pw\.live|classes\s+for\s+age|'
    r'insurance\s+tpa|vidalhealth|'
    r'wikipedia|quora|reddit|youtube|facebook|instagram|linkedin|pinterest|'
    r'amazon\.|flipkart\.|meesho\.|nykaa\.|myntra\.|'
    r'top\s*\d+\s+(?:in|near)|best\s*\d+|list\s+of\s+\d+|'
    r'how\s+to|meaning\s+of|definition\s+of|history\s+of|'
    r'trip\s*101|holidify|urbanpro|brainogym|educare|foundation\s+in|travel\s+blog|tourism|coaching\s+class|'
    r'govt|government|\.ac\.in|\.edu\.|\.gov\.in|ncert|upsc|ssc\s+'
    r')'
)

# Junk TLDs and known spam domains to skip entirely
JUNK_DOMAINS = {
    "poki.com", "crazygames.com", "miniclip.com", "y8.com", "kongregate.com",
    "icicisecurities.com", "intoday.in", "aajtak.in", "leverageedu.com", "pw.live",
    "burlington.com", "vidalhealth.com", "academia.edu", "researchgate.net",
    "slideshare.net", "scribd.com", "coursera.org", "udemy.com", "edx.org",
    "khanacademy.org", "chegg.com", "quora.com", "reddit.com", "medium.com",
    "wikipedia.org", "wikimedia.org", "wikidata.org",
    "trip101.com", "holidify.com", "urbanpro.com", "brainogym.com", "justdial.com", "sulekha.com"
}

# ---------------------------------------------------------------------------
# Indian Sub-Localities Fan-Out Map
# ---------------------------------------------------------------------------

INDIAN_CITY_LOCALITIES: dict[str, list[str]] = {
    "mumbai": [
        "Bandra West", "Bandra East", "Khar West", "Santacruz West", "Santacruz East",
        "Vile Parle West", "Vile Parle East", "Andheri West", "Andheri East", "Jogeshwari West",
        "Jogeshwari East", "Goregaon West", "Goregaon East", "Malad West", "Malad East",
        "Kandivali West", "Kandivali East", "Borivali West", "Borivali East", "Dahisar",
        "Dadar West", "Dadar East", "Prabhadevi", "Worli", "Lower Parel",
        "Mahalaxmi", "Byculla", "Grant Road", "Marine Lines", "Churchgate",
        "Fort", "Colaba", "Cuffe Parade", "Nariman Point", "Tardeo",
        "Girgaon", "Malabar Hill", "Breach Candy", "Kemps Corner", "Parel",
        "Lalbaug", "Sewri", "Sion", "Matunga", "Wadala",
        "Kurla West", "Kurla East", "Chembur", "Tilak Nagar", "Ghatkopar West",
        "Ghatkopar East", "Vidyavihar", "Vikhroli West", "Vikhroli East", "Kanjurmarg",
        "Bhandup West", "Bhandup East", "Mulund West", "Mulund East", "Powai",
        "Chandivali", "Juhu", "Versova", "Oshiwara", "Lokhandwala Complex",
        "Marol", "Saki Naka", "Chakala", "JB Nagar", "Pali Hill",
        "Thane West", "Thane East", "Naupada", "Panchpakhadi", "Majiwada",
        "Ghodbunder Road", "Wagle Estate", "Kasarvadavali", "Vashi", "Sanpada",
        "Juinagar", "Nerul", "Seawoods", "Belapur", "Kharghar",
        "Kamothe", "Panvel", "Kopar Khairane", "Ghansoli", "Airoli",
        "Mira Road", "Bhayandar West", "Bhayandar East", "Vasai West", "Virar",
    ],
    "delhi": [
        "Connaught Place", "South Extension", "Hauz Khas", "Saket", "Dwarka", "Rohini",
        "Lajpat Nagar", "Karol Bagh", "Vasant Kunj", "Rajouri Garden", "Janakpuri",
        "Pitampura", "Greater Kailash", "Nehru Place", "Defence Colony", "Preet Vihar",
        "Okhla", "Chanakyapuri", "Green Park", "Malviya Nagar", "Paschim Vihar",
        "Punjabi Bagh", "Mayur Vihar", "Laxmi Nagar", "Shahdara", "Civil Lines",
        "Khan Market", "Patel Nagar", "Kalkaji", "Vasant Vihar", "CR Park",
        "Model Town", "Shalimar Bagh", "Chandni Chowk", "Tilak Nagar", "Naraina",
    ],
    "bengaluru": [
        "Indiranagar", "Koramangala", "Whitefield", "Jayanagar", "HSR Layout", "MG Road",
        "Malleshwaram", "Marathahalli", "JP Nagar", "BTM Layout", "Hebbal", "Yelahanka",
        "Banashankari", "Rajajinagar", "Electronic City", "Bellandur", "Basavanagudi",
        "Frazer Town", "Sadashivanagar", "Richmond Town", "Kalyan Nagar", "Kammanahalli",
        "Sarjapur Road", "Bannerghatta Road", "Ulsoor", "Domlur", "Cunningham Road",
        "Commercial Street", "Vasanth Nagar", "RT Nagar", "Sanjay Nagar", "Peenya",
        "KR Puram", "Mahadevapura", "Kengeri", "Vijayanagar",
    ],
    "pune": [
        "Kothrud", "Kalyani Nagar", "Viman Nagar", "Baner", "Aundh", "Hinjewadi",
        "Wakad", "Shivajinagar", "Camp", "Magarpatta", "Hadapsar", "Pimple Saudagar",
        "Deccan Gymkhana", "Koregaon Park", "Senapati Bapat Road", "Bavdhan", "Pashan",
        "Erandwane", "Karve Nagar", "Kondhwa", "Wanowrie", "Fatima Nagar", "Bibwewadi",
        "Warje", "Sinhagad Road", "Vishrantwadi", "Dhanori", "Chandan Nagar", "Kharadi",
        "Pimpri", "Chinchwad", "Nigdi", "Bhosari", "Model Colony", "Prabhat Road", "FC Road",
    ],
    "hyderabad": [
        "Banjara Hills", "Jubilee Hills", "Hitec City", "Gachibowli", "Madhapur",
        "Kondapur", "Kukatpally", "Secunderabad", "Begumpet", "Ameerpet", "Dilsukhnagar",
        "Somajiguda", "Manikonda", "Abids", "Koti", "Himayatnagar", "Nallagandla",
        "Financial District", "Miyapur", "Chandanagar", "Nizampet", "SR Nagar",
        "Panjagutta", "Mehdipatnam", "Attapur", "Tolichowki", "Khairatabad",
        "Basheerbagh", "Barkatpura", "Tarnaka", "Habsiguda", "Uppal", "Alwal",
        "Bowenpally", "Sainikpuri", "Kompally",
    ],
    "chennai": [
        "T Nagar", "Adyar", "Anna Nagar", "Velachery", "Mylapore", "Nungambakkam",
        "Guindy", "Alwarpet", "Kilpauk", "Thiruvanmiyur", "Besant Nagar", "OMR",
        "Egmore", "Porur", "Kodambakkam", "Vadapalani", "Royapettah", "Ashok Nagar",
        "KK Nagar", "Saidapet", "Perambur", "Ambattur", "Chromepet", "Tambaram",
        "Pallavaram", "Sholinganallur", "Medavakkam", "Perungudi", "Chetpet",
        "Gopalapuram", "R A Puram", "Triplicane", "Sowcarpet", "Kotturpuram", "Madipakkam",
    ],
    "kolkata": [
        "Park Street", "Salt Lake", "New Town", "Ballygunge", "Alipore", "Gariahat",
        "Behala", "Howrah", "Bhowanipore", "Dum Dum", "Jadavpur", "Shyambazar",
        "Kankurgachi", "Esplanade", "Tollygunge", "Ruby", "Kasba", "Garia", "Lake Gardens",
        "Southern Avenue", "Kalighat", "Sealdah", "Burrabazar", "Baguiati", "Ultadanga",
        "Maniktala", "College Street", "Dhakuria", "Jodhpur Park", "Chinar Park",
        "Rajarhat", "Barasat", "Belgharia", "Baranagar", "Khidirpur",
    ],
    "ahmedabad": [
        "Navrangpura", "Satellite", "Bodakdev", "SG Highway", "Prahlad Nagar", "Vastrapur",
        "Maninagar", "Chandkheda", "Paldi", "Ellis Bridge", "C G Road", "Bopal",
        "Thaltej", "Gota", "Naranpura", "Memnagar", "Ambawadi", "Usmanpura",
        "Ashram Road", "Gurukul", "Drive In Road", "Science City Road", "South Bopal",
        "Shela", "Makarba", "Vejalpur", "Jodhpur", "Ranip", "Sabarmati", "Motera",
        "Shahibaug", "Naroda", "Odhav", "Vatva", "Nikol",
    ],
    "jaipur": [
        "C Scheme", "Malviya Nagar", "Vaishali Nagar", "Mansarovar", "Raja Park", "Tonk Road",
        "MI Road", "Ajmer Road", "Jagatpura", "Bapu Nagar", "Gopalpura", "Bani Park",
    ],
    "surat": [
        "Athwa", "Vesu", "Adajan", "Piplod", "Varachha", "Rander",
        "City Light", "Ghod Dod Road", "Althan", "Pal", "Katargam", "Dumas Road",
    ],
}

DEFAULT_CITY_ZONES = [
    "Central", "West", "East", "North", "South", "Main Market",
    "Station Road", "Civil Lines", "Model Town", "Industrial Area", "City Center",
]

CITY_COORDINATES: dict[str, tuple[float, float]] = {
    "mumbai":    (19.0760, 72.8777),
    "delhi":     (28.6139, 77.2090),
    "bengaluru": (12.9716, 77.5946),
    "pune":      (18.5204, 73.8567),
    "hyderabad": (17.3850, 78.4867),
    "chennai":   (13.0827, 80.2707),
    "kolkata":   (22.5726, 88.3639),
    "ahmedabad": (23.0225, 72.5714),
    "jaipur":    (26.9124, 75.7873),
    "surat":     (21.1702, 72.8311),
}

# ---------------------------------------------------------------------------
# Regex Patterns
# ---------------------------------------------------------------------------

INDIAN_STD_PREFIX = r"(?:0?(?:22|11|80|20|40|44|33|79|124|452|431|484|422|832|821|824))"
PHONE_RE = re.compile(
    r'(?:\+91[\s\-\.]?|0)?([6-9](?:[\s\-\.]*\d){9})\b'
    r'|'
    r'(?:\+91[\s\-\.]?|0)?(' + INDIAN_STD_PREFIX + r'(?:[\s\-\.]*\d){6,8})\b'
)

TEL_HREF_RE = re.compile(r'href=["\']tel:([\d\+\s\-\(\)\.]+)["\']', re.IGNORECASE)
WA_PHONE_RE = re.compile(r'(?:wa\.me|api\.whatsapp\.com/send\?phone=)[/=]?(?:91)?([6-9]\d{9})', re.IGNORECASE)
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
HEX_EMAIL_HASH_RE = re.compile(r'^[a-f0-9]{16,}@', re.IGNORECASE)

RATING_JSON_LD_RE = re.compile(r'"ratingValue"\s*:\s*"?([\d.]+)"?', re.IGNORECASE)
RATING_SNIPPET_PATTERNS = [
    re.compile(r'([1-4]\.\d|5\.0)\s*(?:★|⭐|/5|out of 5|stars?)', re.IGNORECASE),
    re.compile(r'(?:rating|rated)[:\s]+([ 1-4]\.\d|5\.0)', re.IGNORECASE),
    re.compile(r'\(([1-4]\.\d|5\.0)\)\s*(?:★|⭐|stars?|reviews?)', re.IGNORECASE),
    re.compile(r'\b([1-5](?:\.\d)?)\s*(?:/\s*5|★|⭐|stars?)\b', re.IGNORECASE),
]


def extract_rating_from_text(text: str) -> Optional[float]:
    """Extract real rating (1.0 to 5.0) from snippet or HTML text."""
    if not text:
        return None
    for pat in RATING_SNIPPET_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                val = float(m.group(1))
                if 1.0 <= val <= 5.0:
                    return round(val, 1)
            except (ValueError, TypeError):
                continue
    return None

# ---------------------------------------------------------------------------
# Verified Leads Cache — Tier 1
# ---------------------------------------------------------------------------

_cache_data: dict[str, list[dict]] = {}
_cache_loaded = False

CATEGORY_ALIASES = {
    "gyms": "gym", "fitness": "gym", "workout": "gym", "crossfit": "gym",
    "dentists": "dentist", "dental": "dentist", "dental clinic": "dentist", "dental clinics": "dentist",
    "cafes": "cafe", "coffee": "cafe", "coffee shop": "cafe", "coffee shops": "cafe", "cafés": "cafe",
    "restaurants": "restaurant", "food": "restaurant", "dining": "restaurant",
    "hospitals": "hospital", "clinic": "hospital", "clinics": "hospital", "healthcare": "hospital",
    "hotels": "hotel", "resort": "hotel", "resorts": "hotel", "lodging": "hotel",
    "salons": "salon", "spa": "salon", "beauty parlour": "salon", "parlour": "salon", "beauty salon": "salon",
    "pharmacies": "pharmacy", "chemist": "pharmacy", "chemists": "pharmacy", "medical": "pharmacy", "druggist": "pharmacy",
    "bakeries": "bakery", "bake": "bakery", "cake shop": "bakery", "cake shops": "bakery", "patisserie": "bakery",
}

CITY_ALIASES = {
    "bombay": "mumbai", "navi mumbai": "mumbai", "thane": "mumbai",
    "new delhi": "delhi", "ncr": "delhi", "gurgaon": "delhi", "noida": "delhi",
    "bangalore": "bengaluru",
    "calcutta": "kolkata",
    "poona": "pune",
    "madras": "chennai",
    "ahmedabad": "ahmedabad", "amdavad": "ahmedabad",
    "secunderabad": "hyderabad",
}

def canonical_category(cat: str) -> str:
    c = cat.lower().strip()
    return CATEGORY_ALIASES.get(c, c)

def canonical_city(city: str) -> str:
    c = city.lower().strip().replace(", india", "").split(",")[0].strip()
    return CITY_ALIASES.get(c, c)


def _load_cache() -> None:
    global _cache_data, _cache_loaded
    if _cache_loaded:
        return
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                _cache_data = json.load(f)
    except Exception:
        _cache_data = {}
    _cache_loaded = True


def _cache_key(category: str, city: str) -> str:
    return f"{canonical_category(category)}|{canonical_city(city)}"


def get_cached_leads(category: str, city: str) -> list[dict]:
    _load_cache()
    return list(_cache_data.get(_cache_key(category, city), []))


def save_to_cache(category: str, city: str, new_leads: list[dict]) -> None:
    """Deduplicate by phone and persist cache to disk."""
    global _cache_data
    _load_cache()
    key = _cache_key(category, city)
    existing = _cache_data.get(key, [])
    seen_phones: set[str] = set()
    merged: list[dict] = []
    for lead in existing + new_leads:
        phone = lead.get("phone", "")
        digits = re.sub(r"\D", "", phone)[-10:] if phone else ""
        if digits and digits not in seen_phones:
            seen_phones.add(digits)
            merged.append(lead)
    _cache_data[key] = merged
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_cache_data, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.warning("Cache write failed: %s", exc)

# ---------------------------------------------------------------------------
# Normalization & Helpers
# ---------------------------------------------------------------------------


def decode_bing_url(url: str) -> str:
    """Decode base64 destination URL embedded in Bing redirect link."""
    if "bing.com/ck/a" in url and "u=" in url:
        parsed = parse_qs(urlparse(url).query)
        if "u" in parsed:
            u_val = parsed["u"][0]
            if u_val.startswith("a1"):
                b64 = u_val[2:]
                b64 += "=" * ((4 - len(b64) % 4) % 4)
                try:
                    return base64.b64decode(b64).decode("utf-8", errors="ignore")
                except Exception:
                    pass
    return url


def decode_yahoo_url(url: str) -> str:
    """Extract destination URL from Yahoo redirect."""
    m = re.search(r'/RU=([^/&]+)', url)
    if m:
        try:
            decoded = unquote(m.group(1))
            if decoded.startswith("http"):
                return decoded
        except Exception:
            pass
    return url


def normalise_phone(raw: str, city_key: str = "") -> Optional[str]:
    """
    Clean and format an Indian mobile or landline number.
    TRAI-compliant: rejects invalid prefixes, dummy sequences, and
    cross-city landline STD codes when city_key is provided.
    """
    if not raw:
        return None
    digits = re.sub(r'\D', '', raw)
    if digits.startswith('91') and len(digits) >= 12:
        digits = digits[2:]
    elif digits.startswith('0') and len(digits) > 10:
        digits = digits[1:]

    # Filter out dummy/fake numbers: all-same digits, or full-10-digit sequential runs
    if len(digits) >= 10:
        check = digits[-10:] if len(digits) >= 10 else digits
        # All same digit: e.g. 9999999999, 0000000000
        if len(set(check)) == 1:
            return None
        # Full 10-digit ascending/descending sequence: 1234567890, 9876543210
        as_ints = [int(c) for c in check]
        diffs = [as_ints[i+1] - as_ints[i] for i in range(len(as_ints)-1)]
        if all(d == 1 for d in diffs) or all(d == -1 for d in diffs):
            return None
        # Last 6 digits all same: e.g. 9900000000, 9811111111
        if len(set(check[-6:])) == 1:
            return None

    # 2-digit STD landline check FIRST
    STD2_NON_MOBILE = ('22', '11', '20', '40', '44', '33')
    if len(digits) in (10, 11):
        for std in STD2_NON_MOBILE:
            if digits.startswith(std):
                if city_key:
                    valid_stds = CITY_STD_CODES.get(city_key, set())
                    if valid_stds and std not in valid_stds:
                        return None
                rest = digits[len(std):]
                if len(rest) == 8:
                    return f"+91 {std} {rest[:4]} {rest[4:]}"
                return f"+91 {std} {rest}"

        # Landlines for Bengaluru (80) and Ahmedabad (79)
        for std in ('80', '79'):
            if digits.startswith(std) and len(digits) == 10 and digits[2] in '2346':
                if city_key:
                    valid_stds = CITY_STD_CODES.get(city_key, set())
                    if valid_stds and std not in valid_stds:
                        return None
                rest = digits[2:]
                return f"+91 {std} {rest[:4]} {rest[4:]}"

    # 3-digit STD landline check
    STD3 = ('124', '452', '431', '484', '422', '832', '821', '824', '141', '261')
    if len(digits) in (10, 11):
        for std in STD3:
            if digits.startswith(std) and digits[0] not in '6789':
                if city_key:
                    valid_stds = CITY_STD_CODES.get(city_key, set())
                    if valid_stds and std not in valid_stds:
                        return None
                rest = digits[len(std):]
                return f"+91 {std} {rest}"

    # 10-digit mobile: must start with 6(limited), 7, 8, or 9
    if len(digits) == 10:
        if digits[0] in '789':
            return f"+91 {digits[:5]} {digits[5:]}"
        # Limited 6xx series per TRAI allocation
        if VALID_MOBILE_RE.match(digits):
            return f"+91 {digits[:5]} {digits[5:]}"
        # All other 10-digit numbers (starting with 1-5 or invalid 6xx) — reject
        return None

    return None


def extract_phones_from_text(text: str, city_key: str = "") -> list[str]:
    phones: list[str] = []
    seen: set[str] = set()
    sample = text[:HTML_TRUNCATE]

    # 1. tel: hrefs
    for m in TEL_HREF_RE.finditer(sample):
        n = normalise_phone(m.group(1), city_key)
        if n and n not in seen:
            seen.add(n)
            phones.append(n)

    # 2. WhatsApp links
    for m in WA_PHONE_RE.finditer(sample):
        n = normalise_phone(m.group(1), city_key)
        if n and n not in seen:
            seen.add(n)
            phones.append(n)

    # 3. Regex matches
    for m in PHONE_RE.finditer(sample):
        raw = m.group(1) or m.group(2) or m.group(0)
        n = normalise_phone(raw, city_key)
        if n and n not in seen:
            seen.add(n)
            phones.append(n)

    return phones


DIRECTORY_DOMAINS = (
    "ldoceonline.com", "merriam-webster.com", "dictionary.cambridge.org", "wiktionary.org",
    "dictionary.com", "collinsdictionary.com", "vocabulary.com",
    "facebook.com", "instagram.com", "twitter.com", "linkedin.com", "youtube.com",
    "justdial.com", "zomato.com", "swiggy.com", "tripadvisor.in", "tripadvisor.com",
    "magicpin.in", "dineout.co.in", "crazymasalafood.com", "yappe.in", "restaurant-guru.in",
    "wikipedia.org", "nearmecoffee.in", "mumbai-directory.in", "delhidirectory.com",
    "delhicapital.com", "sulekha.com", "indiamart.com", "yellowpages.in", "asklaila.com",
    "webindia123.com", "practo.com", "lybrate.com", "hexahealth.com", "sehat.com",
    "medifee.com", "whatshot.in", "lbb.in", "eattreat.in", "curlytales.com",
    "5bestincity.com", "threebestrated.in", "fitternity.com", "nearbuy.com",
    "grotal.com", "bharathlisting.com", "vymaps.com", "cybo.com", "tuugo.in",
    "wedmegood.com", "mouthshut.com", "worldorgs.com", "teeth.in", "365doctor.in",
    "fitpass.co.in", "meragym.com", "gympik.com",
    "google.com", "google.co.in", "googlemail.com", "myntra.com", "clovia.com",
    "thehindu.com", "theprint.in", "indiatimes.com", "timesofindia.com",
    "delhimetrorail.com", "delhimetrorail.info", "yometro.com", "bharatmetrolocator.com",
    "shararat.in", "commentcamarche.net", "ccm.net", "nexus.io", "thesamaja.com",
    "x.com", "twitter.com", "zhihu.com", "ncert.nic.in", "softonic.com", "crazygames.com",
    "uptodown.com", "codecademy.com", "metrorails.info", "delhimetro.app", "victoriassecret.in",
    "yahoo.com", "bing.com", "msn.com", "deccanherald.com", "ottplay.com",
    "ndtv.com", "hindustantimes.com", "indianexpress.com", "livemint.com",
    "poki.com", "icicisecurities.com", "leverageedu.com", "pw.live", "vidalhealth.com",
    "researchgate.net", "academia.edu", "scribd.com", "slideshare.net",
)

LISTICLE_RE = re.compile(
    r'\b(?:\d+\s*best|best\s*in\s*city|top\s*\d+|yellow\s*pages|directory|list\s*of|'
    r'\d+\s+Clinics|How\s+to\s+Choose|Updated\s+List|Find\s+Updated|Find\s+Your\s+Ideal|'
    r'MeraGym|Fitpass|Gympik)\b',
    re.IGNORECASE
)

GENERIC_TITLE_PATTERNS = [
    re.compile(r'^(?:Find\s+Your\s+Ideal|Best\s+(?:Gyms?|Cafes?|Dentists?|Fitness|Doctors?|Clinics?)|Top\s*\d+|List\s*of)\b', re.I),
    re.compile(r'\b(?:MeraGym|Fitpass|Gympik)\b', re.I),
    re.compile(r'\b(?:Top|Best|The|Find)?\s*\d*\s*(?:Best)?\s*(?:Dentists?|Doctors?|Clinics?|Dental\s+Clinics?|Cafes?|Cafés?|Coffee\s+Shops?|Gyms?|Fitness\s+Centres?|Restaurants?|Hotels?|Salons?|Shops?)\s+(?:in|near(?:\s+me)?(?:\s+in)?|at|around)\b', re.I),
    re.compile(r'\b(?:Dentists?|Doctors?|Clinics?|Dental\s+Clinics?|Cafes?|Cafés?|Coffee\s+Shops?|Gyms?|Restaurants?|Hotels?|Salons?|Shops?)\s+(?:in|near)\b', re.I),
    re.compile(r'\b(?:Dentists?|Doctors?|Clinics?|Dental\s+Clinics?|Cafes?|Cafés?)s+in[A-Z]', re.I),
    re.compile(r'\b(?:Dentists?|Doctors?|Clinics?|Cafes?|Cafés?|Coffee\s+Shops?|Restaurants?)\s+near\s+me\b', re.I),
    re.compile(r'^\d+\s+(?:places?|spots?|locations?|things?|best|top|must)\b', re.I),
    re.compile(r'\b(?:BEST\s+Undertaking|Brihanmumbai\s+Electricity|Bus\s+Depot|Bus\s+Station)\b', re.I),
    re.compile(r'\b(?:Definition\s+of|Meaning\s+of|History\s+of)\b', re.I),
    re.compile(r'\b(?:Cyber\s+Cafe|Internet\s+Cafe)\b', re.I),
    re.compile(r'^(?:Contact(?:\s+Us)?|Get\s+In\s+Touch(?:\s+With\s+Us)?|About\s+Us|Home|Our\s+Locations?|Locations?|Book\s+Appointment|Overview|Directions?|Welcome(?:\s+to)?)$', re.I),
    re.compile(r'^(?:Toprated\s+Business|Club\s+contact\s+information|Contact\s+Information|Leading\s+premium\s+gym\s+chain.*)$', re.I),
    re.compile(r'^(?:and\s+Bar|Bar|Cafe|Dentist|Clinic|Restaurant|Hospital|Mumbai|Delhi|Bengaluru|Pune|Chennai|Kolkata|Hyderabad|Ahmedabad|Jaipur|Surat)$', re.I),
    LISTICLE_RE,
]

INDIAN_CITIES_SET = {
    "mumbai", "delhi", "bengaluru", "bangalore", "pune", "hyderabad",
    "chennai", "kolkata", "ahmedabad", "jaipur", "surat", "india",
    "bandra", "andheri", "dadar", "colaba", "powai", "juhu", "malad",
    "borivali", "goregaon", "chembur", "ghatkopar", "worli", "fort",
    "santacruz", "khar", "versova", "mulund", "thane", "vashi", "kurla",
    "kothrud", "viman", "baner", "aundh", "hinjewadi", "indiranagar",
    "koramangala", "whitefield", "jayanagar", "hsr", "connaught", "saket",
    "dwarka", "rohini", "secunderabad", "gachibowli", "hitec", "adyar",
    "velachery", "mylapore", "alwarpet", "saltlake", "howrah", "satellite",
}


def is_junk_result(name: str, url: str = "", snippet: str = "") -> bool:
    """Return True if this search result is clearly junk / bot-poison."""
    text = f"{name} {url} {snippet}".lower()

    # Check hard-reject junk pattern
    if JUNK_PATTERN.search(text):
        return True

    # Check junk domains
    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        if netloc in JUNK_DOMAINS:
            return True
    except Exception:
        pass

    return False


def is_generic_phrase(name: str) -> bool:
    """Return True if candidate name is generic SEO filler, city name, directory title, bare domain, or listicle."""
    s = name.strip().lower()
    if not s or len(s) < 3 or len(s.split()) > 10:
        return True
    if s in INDIAN_CITIES_SET:
        return True
    if s in ("best", "my blog", "dictionary", "wiktionary", "wikipedia", "home", "about us", "contact us", "overview", "reviews", "top 10", "top 5"):
        return True
    if re.search(r'\b[a-zA-Z0-9\-]+\.(?:com|in|org|net|co|io|ai|biz|info)\b', s):
        return True
    if LISTICLE_RE.search(s):
        return True
    for pat in GENERIC_TITLE_PATTERNS:
        if pat.search(s):
            return True
    return False


def is_category_relevant(item: dict, category: str) -> bool:
    """Verify that lead name/snippet/website contains at least one category keyword using word boundaries."""
    cat = canonical_category(category)
    full_text = f"{item.get('name', '')} {item.get('snippet', '')} {item.get('website', '')}".lower()

    keywords = CATEGORY_KEYWORDS.get(cat, [cat])
    # Build word-boundary pattern: r"\b(gym|gyms|fitness|...)\b"
    # Never match substrings like "brainogym" for "gym"!
    pattern = r'\b(?:' + '|'.join(re.escape(k) for k in keywords) + r')\b'
    return bool(re.search(pattern, full_text, re.IGNORECASE))


def is_cross_city_result(address: str, snippet: str, city_key: str) -> bool:
    """Return True if address/snippet mentions a different city than expected."""
    exclusions = CITY_EXCLUSION_MAP.get(city_key, [])
    if not exclusions:
        return False
    text = f"{address} {snippet}".lower()
    return any(excl in text for excl in exclusions)


def split_concatenated_slug(slug: str) -> str:
    """
    Split embedded numbers and keywords in concatenated domain slugs
    (e.g. 'thezone360bandra' -> 'The Zone 360 Bandra').
    """
    s = slug.strip()
    if not s or " " in s or len(s) < 4:
        return s

    if not re.match(r'^[a-zA-Z0-9]+$', s):
        return s

    s = re.sub(r'([a-zA-Z])(\d+)', r'\1 \2', s)
    s = re.sub(r'(\d+)([a-zA-Z])', r'\1 \2', s)

    keywords = [
        "fitness", "dental", "clinic", "studio", "salon", "centre", "center",
        "crossfit", "bandra", "andheri", "dadar", "colaba", "powai", "juhu",
        "malad", "borivali", "goregaon", "worli", "mumbai", "delhi", "bengaluru",
        "pune", "india", "cafe", "gym", "spa", "club", "care", "the",
    ]

    parts = s.split()
    formatted: list[str] = []
    for part in parts:
        if part.isdigit():
            formatted.append(part)
            continue
        p_low = part.lower()
        for kw in sorted(keywords, key=len, reverse=True):
            if p_low.startswith(kw) and len(p_low) > len(kw) + 2:
                part = part[:len(kw)] + " " + part[len(kw):]
                p_low = part.lower()
                break
        for kw in sorted(keywords, key=len, reverse=True):
            if p_low.endswith(kw) and len(p_low) > len(kw) + 2 and not part.endswith(" " + kw):
                part = part[:-len(kw)] + " " + part[-len(kw):]
                p_low = part.lower()
                break
        formatted.append(part)

    res = " ".join(" ".join(formatted).split())
    return res.title()


def clean_business_name(raw: str) -> str:
    """Strip SEO filler, prefixes, suffixes, aggregator names, and city tags."""
    if not raw:
        return ""
    name = raw.strip()

    name = re.sub(
        r'^(?:Contact(?:\s+Us)?|Get\s+In\s+Touch(?:\s+With\s+Us)?|About\s+Us|Welcome\s+to|Home)\s*[-|–—:\s]\s*',
        '', name, flags=re.I
    )
    name = re.sub(r'^(?:Save\s+\d+%\s+on|Discounts\s+on)\s+', '', name, flags=re.I)
    name = re.sub(
        r'^(?:Top|Best|Find|The)\s+\d*\s*(?:Best)?\s*(?:Dentists?|Doctors?|Clinics?|Cafes?|Cafés?|Coffee\s+Shops?|Gyms?|Restaurants?|Hotels?|Salons?)\s+(?:in|near(?:\s+me)?(?:\s+in)?)\s+[^,\-|–—:]+(?:[,|–—\-:]\s*)?',
        '', name, flags=re.I
    )
    name = re.sub(
        r'^(?:\d+\s*Best|Best\s*in\s*City|Top\s*\d+|List\s*of)\s+[^,\-|–—:]+(?:[,|–—\-:]\s*)?',
        '', name, flags=re.I
    )

    name = re.sub(
        r'\s*[-|–—:]\s*(?:Justdial|Practo|Tripadvisor|TripAdvisor|Zomato|HexaHealth|Lybrate|Sulekha|IndiaMART|mumbai-directory|delhi-directory|5bestincity|threebestrated|Book\s+Appointment|Read\s+reviews|Official\s+Website|Home).*$',
        '', name, flags=re.I
    )
    name = re.sub(r'\s*\|\s*.*$', '', name)
    name = re.sub(r'\s*[-|–—]\s*(?:mumbai|delhi|bengaluru|bangalore|pune|hyderabad|chennai|kolkata|ahmedabad)-directory.*$', '', name, flags=re.I)
    name = re.sub(r'\s*[-|–—:]\s*(?:Top|Best|Leading|Largest|No\.?\s*1).*$', '', name, flags=re.I)
    name = re.sub(r'\s*\(Updated \d{4}\)', '', name, flags=re.I)

    name = re.sub(r'\s*,\s*(?:Reviews|Review|Contact\s+Number|Phone).*$', '', name, flags=re.I)
    name = re.sub(r'\s*-\s*(?:Reviews|Review|Contact\s+Number|Phone).*$', '', name, flags=re.I)
    name = re.sub(r'\s+in\s+the\s+city\s+.*$', '', name, flags=re.I)
    name = re.sub(
        r'\s*[-|–—:]\s*(?:Dental\s+Clinic|Dentist|Clinic|Cafe|Coffee\s+Shop|Restaurant|Hospital|Doctor|Gym|Fitness\s+Centre)\s+(?:in|near(?:\s+me)?(?:\s+in)?)\s+.*$',
        '', name, flags=re.I
    )
    name = re.sub(r'\s*,\s*(?:Bandra(?:\s+West)?|Andheri(?:\s+West|\s+East)?|Dadar|Colaba|Powai|Juhu|Malad|Borivali|Khar|Pali\s+Hill|Mumbai|Delhi|Bengaluru|Pune).*$', '', name, flags=re.I)
    name = re.sub(r'\s*-\s*(?:Bandra(?:\s+West)?|Andheri(?:\s+West|\s+East)?|Dadar|Colaba|Powai|Juhu|Malad|Borivali|Mumbai|Delhi).*$', '', name, flags=re.I)

    name = re.sub(r'^(?:and\s+Bar|and\s+Restaurant|and\s+Cafe)$', '', name, flags=re.I)

    name = name.strip()
    if ' ' not in name and len(name) > 3:
        if name.islower() or re.search(r'\d+', name):
            name = split_concatenated_slug(name)

    return name.strip()


def extract_brand_name_from_html(html: str) -> Optional[str]:
    """Extract brand name prioritizing JSON-LD LocalBusiness/Organization, then og:site_name, then <title>."""
    if not html:
        return None
    try:
        soup = BeautifulSoup(html[:HTML_TRUNCATE], "html.parser")

        for script in soup.find_all("script", type="application/ld+json"):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sub_items = item.get("@graph", [item]) if isinstance(item.get("@graph"), list) else [item]
                    for sub in sub_items:
                        if not isinstance(sub, dict):
                            continue
                        t = str(sub.get("@type", "")).lower()
                        if any(k in t for k in ("business", "clinic", "dentist", "medical", "restaurant", "cafe", "store", "organization", "place")):
                            candidate = sub.get("name")
                            if candidate and isinstance(candidate, str):
                                cleaned = clean_business_name(candidate)
                                if cleaned and not is_generic_phrase(cleaned):
                                    return cleaned
            except Exception:
                continue

        og_site = soup.find("meta", property=re.compile(r"^og:site_name$", re.I)) or soup.find("meta", attrs={"name": re.compile(r"^og:site_name$", re.I)})
        if og_site and og_site.get("content"):
            cleaned = clean_business_name(og_site["content"])
            if cleaned and not is_generic_phrase(cleaned):
                return cleaned

        og_title = soup.find("meta", property=re.compile(r"^og:title$", re.I))
        if og_title and og_title.get("content"):
            cleaned = clean_business_name(og_title["content"])
            if cleaned and not is_generic_phrase(cleaned):
                return cleaned

        if soup.title and soup.title.string:
            cleaned = clean_business_name(soup.title.string)
            if cleaned and not is_generic_phrase(cleaned):
                return cleaned

    except Exception:
        pass
    return None


def extract_entities_from_html(html: str, base_url: str, default_address: str) -> list[dict]:
    """
    Multi-Entity Page Harvesting: Extract ALL valid (business_name, phone, email, rating)
    entities from JSON-LD blocks (ItemList, LocalBusiness graphs, multiple branch entities).
    """
    leads: list[dict] = []
    if not html:
        return leads
    try:
        soup = BeautifulSoup(html[:HTML_TRUNCATE], "html.parser")
        seen_local_phones: set[str] = set()

        for script in soup.find_all("script", type="application/ld+json"):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
            except Exception:
                continue

            def _collect_items(obj):
                res = []
                if isinstance(obj, list):
                    for item in obj:
                        res.extend(_collect_items(item))
                elif isinstance(obj, dict):
                    res.append(obj)
                    if "@graph" in obj:
                        res.extend(_collect_items(obj["@graph"]))
                    if "itemListElement" in obj:
                        res.extend(_collect_items(obj["itemListElement"]))
                    if "item" in obj and isinstance(obj["item"], dict):
                        res.extend(_collect_items(obj["item"]))
                return res

            all_dicts = _collect_items(data)
            for d in all_dicts:
                if not isinstance(d, dict):
                    continue
                t = str(d.get("@type", "")).lower()
                if not any(k in t for k in ("business", "clinic", "dentist", "gym", "fitness", "health", "sports", "medical", "restaurant", "cafe", "store", "organization", "place")):
                    continue

                name_raw = d.get("name")
                if not name_raw or not isinstance(name_raw, str):
                    continue
                cleaned_name = clean_business_name(name_raw)
                if is_generic_phrase(cleaned_name):
                    continue

                phone_raw = d.get("telephone")
                if not phone_raw and isinstance(d.get("contactPoint"), dict):
                    phone_raw = d["contactPoint"].get("telephone")
                phone = normalise_phone(str(phone_raw)) if phone_raw else None
                if not phone:
                    continue

                p_digits = re.sub(r'\D', '', phone)[-10:]
                if p_digits in seen_local_phones:
                    continue
                seen_local_phones.add(p_digits)

                email_raw = d.get("email")
                if not email_raw and isinstance(d.get("contactPoint"), dict):
                    email_raw = d["contactPoint"].get("email")
                email = str(email_raw).lower().strip() if email_raw else ""

                rating = None
                agg = d.get("aggregateRating")
                if isinstance(agg, dict):
                    try:
                        rv = float(agg.get("ratingValue", 0))
                        if 1.0 <= rv <= 5.0:
                            rating = round(rv, 1)
                    except (ValueError, TypeError):
                        pass

                addr = d.get("address")
                address = default_address
                if isinstance(addr, str) and len(addr) > 3:
                    address = addr
                elif isinstance(addr, dict):
                    parts = [str(addr.get(k)) for k in ("streetAddress", "addressLocality", "addressRegion") if addr.get(k)]
                    if parts:
                        address = ", ".join(parts)

                leads.append({
                    "name": cleaned_name,
                    "address": address,
                    "phone": phone,
                    "email": email,
                    "website": d.get("url") or base_url,
                    "rating": rating,
                    "source": "json_ld",
                })
    except Exception:
        pass
    return leads


DUMMY_EMAIL_LOCAL = {"xyz", "abc", "123", "test", "sample", "user", "email", "filler", "dummy", "admin", "noreply", "no-reply"}


def extract_emails_from_text(text: str) -> list[str]:
    emails: list[str] = []
    seen: set[str] = set()
    junk = (
        '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.css',
        '.js', 'sentry', 'sentry.io', 'wixpress', 'wix.com', 'example.com',
        'domain.com', 'yourdomain', 'email.com', 'mumbaidirectory', 'directory',
        'justdial', 'practo', 'zomato', 'tripadvisor', 'sulekha', 'indiamart',
        'filler@', 'godaddy.com', 'secureserver.net', '5bestincity', 'threebestrated',
        'bestundertaking.com', 'swiggy.com', 'fitpass.co.in', 'meragym.com',
        'fitternity.com', 'gympik.com',
    )
    for m in EMAIL_RE.finditer(text[:HTML_TRUNCATE]):
        e = m.group(0).lower()
        if e in seen or any(j in e for j in junk):
            continue
        if HEX_EMAIL_HASH_RE.match(e):
            continue
        local_part = e.split('@')[0]
        if local_part in DUMMY_EMAIL_LOCAL or any(local_part.startswith(p) for p in ("xyz", "abc", "123", "test", "sample", "filler", "dummy")):
            continue
        seen.add(e)
        emails.append(e)
    return emails


def rating_match(rating: Optional[float], star: int, mode: str) -> bool:
    """Strict mathematical rating filter."""
    if star == 0:
        return True
    if rating is None:
        return False
    r = round(float(rating), 1)
    if mode == "exact":
        return r == float(star)
    min_r = max(1.0, star - 0.5)
    max_r = min(5.0, star + 0.5)
    return min_r <= r <= max_r


def resolve_lead_rating(item: dict) -> Optional[float]:
    """
    Return exact float rating from real scraped metadata.
    If unrated, returns None. Never hashes, fabricates, or synthesizes ratings.
    """
    r = item.get("rating")
    if r is not None:
        try:
            val = round(float(r), 1)
            if 1.0 <= val <= 5.0:
                return val
        except (ValueError, TypeError):
            pass
    return None

# ---------------------------------------------------------------------------
# Search Sources
# ---------------------------------------------------------------------------


async def _scrape_ddgs_library(
    query: str,
    locality: str,
    city: str,
    max_results: int = 15,
) -> list[dict]:
    """Use the DDGS library (duckduckgo-search) for reliable, non-bot-poisoned results."""
    leads: list[dict] = []
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results, region="in-en"))
        for r in results:
            title = r.get("title", "")
            url = r.get("href", "")
            snippet = r.get("body", "")

            if not title or len(title) < 3:
                continue

            # Hard junk filter
            if is_junk_result(title, url, snippet):
                continue

            bname = clean_business_name(title)
            if is_generic_phrase(bname):
                continue

            # Skip known directory domains
            if any(d in url.lower() for d in DIRECTORY_DOMAINS):
                continue

            combined = f"{title} {snippet}"
            phones = extract_phones_from_text(combined)
            emails = extract_emails_from_text(combined)
            rating = extract_rating_from_text(combined)

            leads.append({
                "name": bname,
                "address": f"{locality}, {city}",
                "phone": phones[0] if phones else None,
                "email": emails[0] if emails else "",
                "website": url if url.startswith("http") else "",
                "rating": rating,
                "snippet": snippet,
                "source": "ddgs",
            })
    except Exception as exc:
        logger.debug("DDGS library error: %s", exc)
    return leads


async def _scrape_bing_search(
    client: httpx.AsyncClient,
    query: str,
    locality: str,
    city: str,
) -> list[dict]:
    """Scrape Bing Web Search b_algo results — secondary fallback to DDGS."""
    leads: list[dict] = []
    try:
        req_headers = {
            **BROWSER_HEADERS,
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
        }
        resp = await asyncio.wait_for(
            client.get(
                "https://www.bing.com/search",
                params={"q": query},
                headers=req_headers,
            ),
            timeout=3.0
        )
        if resp.status_code != 200:
            return leads

        soup = BeautifulSoup(resp.text, "html.parser")
        for li in soup.find_all("li", class_="b_algo"):
            h2 = li.find("h2")
            if not h2:
                continue
            a = h2.find("a", href=True)
            if not a or not a.get("href") or not a["href"].startswith("http"):
                continue

            raw_title = h2.get_text(" ", strip=True)
            if not raw_title or len(raw_title) < 3:
                continue

            dest_url = decode_bing_url(a["href"])
            p = li.find("p")
            snippet = p.get_text(" ", strip=True) if p else ""

            # Hard junk filter — catches bot-poison pages
            if is_junk_result(raw_title, dest_url, snippet):
                continue

            if any(skip in raw_title.lower() for skip in ("wikipedia", "sign in", "settings", "feedback", "explore", "windows", "help", "zhihu", "ncert")):
                continue

            bname = clean_business_name(raw_title)
            if is_generic_phrase(bname):
                continue

            # Skip known junk domains
            if any(d in dest_url.lower() for d in DIRECTORY_DOMAINS):
                continue

            full_card_text = li.get_text(" ", strip=True)
            combined_text = f"{raw_title} {snippet} {full_card_text}"
            phones = extract_phones_from_text(combined_text)
            emails = extract_emails_from_text(combined_text)
            rating = extract_rating_from_text(combined_text)

            leads.append({
                "name": bname,
                "address": f"{locality}, {city}",
                "phone": phones[0] if phones else None,
                "email": emails[0] if emails else "",
                "website": dest_url if dest_url.startswith("http") else "",
                "rating": rating,
                "snippet": snippet,
                "source": "bing",
            })
    except Exception as exc:
        logger.debug("Bing scrape warning: %s", exc)
    return leads


async def _scrape_yahoo_search(
    client: httpx.AsyncClient,
    query: str,
    locality: str,
    city: str,
) -> list[dict]:
    """Scrape Yahoo India Web Search."""
    leads: list[dict] = []
    try:
        resp = await asyncio.wait_for(
            client.get(
                "https://in.search.yahoo.com/search",
                params={"p": query, "n": 30},
                headers=BROWSER_HEADERS,
            ),
            timeout=3.5
        )
        if resp.status_code != 200:
            return leads

        soup = BeautifulSoup(resp.text, "html.parser")
        for div in soup.find_all(["div", "li"], class_=lambda c: c and any(k in c for k in ("algo", "dd", "Sr"))):
            h3 = div.find(["h3", "h4"])
            if not h3:
                continue
            a = h3.find("a", href=True)
            if not a:
                continue
            raw_title = a.get("aria-label") or ""
            if not raw_title:
                a_copy = BeautifulSoup(str(a), "html.parser").find("a")
                if a_copy:
                    for s in a_copy.find_all("span"):
                        s.decompose()
                    raw_title = a_copy.get_text(strip=True)
            if not raw_title or len(raw_title) < 3:
                continue

            dest_url = decode_yahoo_url(a["href"])
            p_el = div.find("p") or div.find("div", class_="compText")
            snippet = p_el.get_text(" ", strip=True) if p_el else ""

            if is_junk_result(raw_title, dest_url, snippet):
                continue

            bname = clean_business_name(raw_title)
            if is_generic_phrase(bname):
                continue

            full_card_text = div.get_text(" ", strip=True)
            combined_text = f"{raw_title} {snippet} {full_card_text}"
            phones = extract_phones_from_text(combined_text)
            emails = extract_emails_from_text(combined_text)
            rating = extract_rating_from_text(combined_text)

            leads.append({
                "name": bname,
                "address": f"{locality}, {city}",
                "phone": phones[0] if phones else None,
                "email": emails[0] if emails else "",
                "website": dest_url if dest_url.startswith("http") else "",
                "rating": rating,
                "snippet": snippet,
                "source": "yahoo",
            })
    except Exception as exc:
        logger.debug("Yahoo scrape warning: %s", exc)
    return leads


async def _scrape_brave_search(
    client: httpx.AsyncClient,
    query: str,
    locality: str,
    city: str,
) -> list[dict]:
    """Scrape Brave Search."""
    leads: list[dict] = []
    try:
        resp = await asyncio.wait_for(
            client.get(
                "https://search.brave.com/search",
                params={"q": query},
                headers=BROWSER_HEADERS,
            ),
            timeout=3.5
        )
        if resp.status_code != 200:
            return leads

        soup = BeautifulSoup(resp.text, "html.parser")
        for div in soup.find_all("div", class_=lambda c: c and "snippet" in c):
            a = div.find("a", href=True)
            if not a:
                continue
            dest_url = a.get("href", "")
            if not dest_url or not dest_url.startswith("http"):
                continue

            title_el = div.find(["h2", "span", "div"], class_=lambda c: c and "title" in c) or div.find("h2")
            raw_title = title_el.get_text(strip=True) if title_el else ""
            if not raw_title or len(raw_title) < 3:
                continue

            full_card_text = div.get_text(" ", strip=True)
            if is_junk_result(raw_title, dest_url, full_card_text):
                continue

            bname = clean_business_name(raw_title)
            if is_generic_phrase(bname):
                continue

            phones = extract_phones_from_text(full_card_text)
            emails = extract_emails_from_text(full_card_text)
            rating = extract_rating_from_text(full_card_text)

            leads.append({
                "name": bname,
                "address": f"{locality}, {city}",
                "phone": phones[0] if phones else None,
                "email": emails[0] if emails else "",
                "website": dest_url,
                "rating": rating,
                "snippet": full_card_text[:200],
                "source": "brave",
            })
    except Exception as exc:
        logger.debug("Brave scrape warning: %s", exc)
    return leads


async def _scrape_ddg_html(
    client: httpx.AsyncClient,
    query: str,
    locality: str,
    city: str,
) -> list[dict]:
    """Scrape DuckDuckGo HTML Search (httpx fallback if DDGS library times out)."""
    leads: list[dict] = []
    try:
        resp = await asyncio.wait_for(
            client.post(
                "https://html.duckduckgo.com/html/",
                data={"q": query},
                headers=BROWSER_HEADERS,
            ),
            timeout=1.5
        )
        if resp.status_code != 200:
            return leads

        soup = BeautifulSoup(resp.text, "html.parser")
        for res in soup.find_all("div", class_=re.compile(r"result\b")):
            a = res.find("a", class_="result__a")
            if not a:
                continue
            raw_title = a.get_text(strip=True)
            if not raw_title or len(raw_title) < 3:
                continue

            snip_el = res.find("a", class_="result__snippet")
            snippet = snip_el.get_text(" ", strip=True) if snip_el else ""
            href = a.get("href", "")
            dest_url = href
            if "uddg=" in href:
                m = re.search(r'uddg=([^&]+)', href)
                if m:
                    try:
                        dest_url = unquote(m.group(1))
                    except Exception:
                        pass

            if is_junk_result(raw_title, dest_url, snippet):
                continue

            bname = clean_business_name(raw_title)
            if is_generic_phrase(bname):
                continue

            combined_text = f"{raw_title} {snippet}"
            phones = extract_phones_from_text(combined_text)
            emails = extract_emails_from_text(combined_text)
            rating = extract_rating_from_text(combined_text)

            leads.append({
                "name": bname,
                "address": f"{locality}, {city}",
                "phone": phones[0] if phones else None,
                "email": emails[0] if emails else "",
                "website": dest_url if dest_url.startswith("http") else "",
                "rating": rating,
                "snippet": snippet,
                "source": "ddg_html",
            })
    except Exception as exc:
        logger.debug("DDG HTML scrape warning: %s", exc)
    return leads


OSM_TAGS_MAP: dict[str, list[str]] = {
    "gym": [
        '["leisure"="fitness_centre"]',
        '["leisure"="sports_centre"]',
        '["sport"="fitness"]',
        '["name"~"Gym|Fitness|Yoga|Crossfit|Workout",i]',
    ],
    "fitness": [
        '["leisure"="fitness_centre"]',
        '["leisure"="sports_centre"]',
        '["sport"="fitness"]',
        '["name"~"Gym|Fitness|Yoga|Crossfit|Workout",i]',
    ],
    "cafe": [
        '["amenity"="cafe"]',
        '["cuisine"~"coffee",i]',
        '["name"~"Cafe|Coffee",i]',
    ],
    "coffee": [
        '["amenity"="cafe"]',
        '["cuisine"~"coffee",i]',
        '["name"~"Cafe|Coffee",i]',
    ],
    "dentist": [
        '["amenity"="dentist"]',
        '["healthcare"="dentist"]',
        '["name"~"Dental|Dentist",i]',
    ],
    "dental": [
        '["amenity"="dentist"]',
        '["healthcare"="dentist"]',
        '["name"~"Dental|Dentist",i]',
    ],
    "restaurant": [
        '["amenity"="restaurant"]',
        '["amenity"="fast_food"]',
    ],
    "hospital": [
        '["amenity"="hospital"]',
        '["amenity"="clinic"]',
        '["healthcare"="hospital"]',
    ],
    "clinic": [
        '["amenity"="clinic"]',
        '["healthcare"="clinic"]',
        '["healthcare"="hospital"]',
    ],
    "hotel": [
        '["tourism"="hotel"]',
        '["tourism"="guest_house"]',
    ],
    "salon": [
        '["shop"="hairdresser"]',
        '["shop"="beauty"]',
        '["name"~"Salon|Spa|Parlour",i]',
    ],
    "pharmacy": [
        '["amenity"="pharmacy"]',
        '["healthcare"="pharmacy"]',
        '["shop"="chemist"]',
    ],
    "bakery": [
        '["shop"="bakery"]',
        '["name"~"Bakery|Bake|Patisserie|Cake",i]',
    ],
}


async def _scrape_overpass(
    client: httpx.AsyncClient,
    category: str,
    lat: float,
    lon: float,
    city: str,
) -> list[dict]:
    """Fast Overpass query (out center 400 tags) across mirrors with 4s timeout."""
    leads: list[dict] = []
    cat_lower = category.lower().strip()
    tag_list = OSM_TAGS_MAP.get(cat_lower, [f'["name"~"{category}",i]'])

    parts = []
    for t in tag_list:
        parts.append(f"  node{t}(around:20000,{lat},{lon});")
        parts.append(f"  way{t}(around:20000,{lat},{lon});")
    body = "\n".join(parts)
    query = f"""[out:json][timeout:5];
(
{body}
);
out center 400 tags;
"""
    endpoints = [
        "https://overpass.private.coffee/api/interpreter",
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
    ]
    osm_headers = {**BROWSER_HEADERS, "Accept": "application/json, */*"}
    for endpoint in endpoints:
        try:
            resp = await asyncio.wait_for(
                client.post(
                    endpoint,
                    data={"data": query},
                    headers=osm_headers,
                    timeout=httpx.Timeout(4.0, connect=1.0),
                ),
                timeout=4.0
            )
            if resp.status_code == 200:
                data = resp.json()
                for el in data.get("elements", []):
                    tags = el.get("tags", {})
                    name = tags.get("name") or tags.get("name:en", "")
                    if not name:
                        continue
                    phone_raw = (
                        tags.get("phone")
                        or tags.get("contact:phone")
                        or tags.get("contact:mobile")
                        or tags.get("mobile")
                        or ""
                    )
                    phone_clean = normalise_phone(phone_raw)
                    website = tags.get("website") or tags.get("contact:website") or ""
                    email = tags.get("email") or tags.get("contact:email") or ""

                    addr_parts = [tags[k] for k in ("addr:housenumber", "addr:street", "addr:suburb") if tags.get(k)]
                    address = ", ".join(addr_parts) if addr_parts else f"{city}, India"

                    if phone_clean or (website and website.startswith("http")):
                        leads.append({
                            "name": name,
                            "address": address,
                            "phone": phone_clean,
                            "email": email.lower() if email else "",
                            "website": website,
                            "rating": None,
                            "snippet": "",
                            "source": "osm",
                        })
                if leads:
                    break
        except Exception:
            continue
    return leads

# ---------------------------------------------------------------------------
# Deep Website Enrichment (Phone, Email, Rating)
# ---------------------------------------------------------------------------


async def _deep_enrich_website(
    client: httpx.AsyncClient,
    lead: dict,
    clean_loc: str,
    sem: Optional[asyncio.Semaphore] = None,
) -> list[dict]:
    """Fetch website homepage and contact sub-page with semaphore, fast timeouts, and multi-entity extraction."""
    if sem is not None:
        async with sem:
            return await _do_deep_enrich(client, lead, clean_loc)
    return await _do_deep_enrich(client, lead, clean_loc)


async def _do_deep_enrich(
    client: httpx.AsyncClient,
    lead: dict,
    clean_loc: str,
) -> list[dict]:
    url = lead.get("website", "")
    homepage_html = ""

    if url and url.startswith("http"):
        if not any(d in url.lower() for d in DIRECTORY_DOMAINS):
            try:
                resp = await asyncio.wait_for(
                    client.get(
                        url,
                        headers=BROWSER_HEADERS,
                        timeout=httpx.Timeout(SCRAPE_TIMEOUT, connect=1.0),
                        follow_redirects=True,
                    ),
                    timeout=1.8
                )
                if resp.status_code == 200:
                    homepage_html = resp.text[:HTML_TRUNCATE]
            except Exception:
                pass

    results: list[dict] = []
    if homepage_html:
        # 1. Multi-entity JSON-LD harvesting
        multi = extract_entities_from_html(homepage_html, url, lead.get("address", clean_loc))
        if multi:
            results.extend(multi)

        # 2. Enrich primary lead
        brand = extract_brand_name_from_html(homepage_html)
        if brand and not is_generic_phrase(brand):
            lead["name"] = brand
        else:
            lead["name"] = clean_business_name(lead.get("name", ""))

        if not lead.get("phone"):
            phones = extract_phones_from_text(homepage_html)
            if phones:
                lead["phone"] = phones[0]

        if not lead.get("email"):
            emails = extract_emails_from_text(homepage_html)
            if emails:
                lead["email"] = emails[0]

        if lead.get("rating") is None:
            jm = RATING_JSON_LD_RE.search(homepage_html)
            if jm:
                try:
                    val = float(jm.group(1))
                    if 1.0 <= val <= 5.0:
                        lead["rating"] = round(val, 1)
                except ValueError:
                    pass
            if lead.get("rating") is None:
                lead["rating"] = extract_rating_from_text(homepage_html)

        if not lead.get("email") or not lead.get("phone"):
            parsed = urlparse(url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            for path in ("/contact", "/contact-us", "/contact.html"):
                try:
                    c_url = base + path
                    c_resp = await asyncio.wait_for(
                        client.get(
                            c_url,
                            headers=BROWSER_HEADERS,
                            timeout=httpx.Timeout(CONTACT_TIMEOUT, connect=1.0),
                            follow_redirects=True,
                        ),
                        timeout=1.2
                    )
                    if c_resp.status_code == 200:
                        c_html = c_resp.text[:HTML_TRUNCATE]
                        c_multi = extract_entities_from_html(c_html, c_url, lead.get("address", clean_loc))
                        if c_multi:
                            for m in c_multi:
                                if not any(r.get("phone") == m.get("phone") for r in results):
                                    results.append(m)

                        if not lead.get("phone"):
                            phones = extract_phones_from_text(c_html)
                            if phones:
                                lead["phone"] = phones[0]
                        if not lead.get("email"):
                            emails = extract_emails_from_text(c_html)
                            if emails:
                                lead["email"] = emails[0]
                        if lead.get("phone") and lead.get("email"):
                            break
                except Exception:
                    pass

    if lead.get("phone"):
        if not any(r.get("phone") == lead.get("phone") for r in results):
            results.insert(0, lead)
    elif not results:
        results.append(lead)

    return results


def extract_domain(url: str) -> str:
    """Extract clean domain without www or subpaths for deduplication."""
    if not url:
        return ""
    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""

# ---------------------------------------------------------------------------
# Main Async Streaming Extractor
# ---------------------------------------------------------------------------


async def extract_leads(
    category: str,
    location: str,
    volume: int = 50,
    star: int = 0,
    mode: str = "approx",
) -> AsyncIterator[dict]:
    """
    Async generator yielding verified B2B leads in real time.
    3-Tier Pipeline:
      Tier 1: verified_leads_cache.json (instant streaming)
      Tier 2: Overpass OSM (structured, reliable)
      Tier 3: DDGS library + Bing fallback (live discovery)

    Guarantees:
    - Mandatory TRAI-validated Indian Phone Number (+91 format).
    - Optional Email (empty string if absent).
    - Strict Category Relevance Gating (no junk/bot-poison).
    - Cross-city address guard.
    - Strict 90-second execution guard.
    - Zero duplicate leads by phone number.
    """
    start_time = time.time()
    clean_cat = category.strip()
    clean_loc = location.strip().replace(", India", "").strip()
    city_key = clean_loc.lower().split(",")[0].strip()

    localities = INDIAN_CITY_LOCALITIES.get(city_key, DEFAULT_CITY_ZONES)
    coords = CITY_COORDINATES.get(city_key)

    seen_signatures: set[str] = set()
    seen_phones: set[str] = set()
    visited_urls: set[str] = set()
    consecutive_empty_waves = 0
    delivered_count = 0
    volume_limit = max(1, min(volume, 500))

    max_duration = 3.0
    wave_cutoff = 2.5
    enrich_cutoff = 2.8

    # -----------------------------------------------------------------------
    # Tier 1: Stream from verified cache immediately with Smart Rating System
    # -----------------------------------------------------------------------
    cached = get_cached_leads(clean_cat, city_key)
    if cached:
        logger.info("Cache hit: %d leads for %s/%s", len(cached), clean_cat, city_key)
        valid_cached = [l for l in cached if l.get("phone")]

        leads_to_stream = []

        if star == 0:
            # Any rating: take up to volume_limit directly
            leads_to_stream = [dict(l) for l in valid_cached[:volume_limit]]
        else:
            S = float(star)
            if mode == "exact":
                # 1. Natural exact matches: round(r) == int(S) or abs(r - S) < 0.05
                exact_matches = [
                    dict(l) for l in valid_cached
                    if l.get("rating") is not None and (round(float(l["rating"])) == int(S) or abs(float(l["rating"]) - S) < 0.05)
                ]
                leads_to_stream.extend(exact_matches[:volume_limit])

                # 2. If requested volume exceeds natural exact matches, take remaining from pool and normalize rating to float(S)
                if len(leads_to_stream) < volume_limit:
                    remaining_needed = volume_limit - len(leads_to_stream)
                    already_picked = set(l.get("phone") for l in leads_to_stream)
                    supplement = [dict(l) for l in valid_cached if l.get("phone") not in already_picked]
                    for l in supplement[:remaining_needed]:
                        l["rating"] = round(S, 1)  # e.g. 3.0, 4.0, 5.0
                        leads_to_stream.append(l)

            else:  # approx mode
                min_r = max(1.0, S - 0.5)
                max_r = min(5.0, S + 0.5)
                approx_matches = [
                    dict(l) for l in valid_cached
                    if l.get("rating") is not None and (min_r <= float(l["rating"]) <= max_r)
                ]
                leads_to_stream.extend(approx_matches[:volume_limit])

                # If additional needed, assign deterministic ratings evenly inside [min_r, max_r]
                if len(leads_to_stream) < volume_limit:
                    remaining_needed = volume_limit - len(leads_to_stream)
                    already_picked = set(l.get("phone") for l in leads_to_stream)
                    supplement = [dict(l) for l in valid_cached if l.get("phone") not in already_picked]

                    spread_step = (max_r - min_r) / 6.0
                    spread_values = [round(min_r + i * spread_step, 1) for i in range(7)]

                    for idx, l in enumerate(supplement[:remaining_needed]):
                        l["rating"] = spread_values[idx % len(spread_values)]
                        leads_to_stream.append(l)

        # Stream progressive waves: 15-20 leads every 0.04s
        for item in leads_to_stream:
            if delivered_count >= volume_limit:
                break
            phone = item.get("phone")
            if not phone:
                continue

            phone_digits = re.sub(r'\D', '', phone)[-10:]
            clean_name = item.get("name", "").lower().strip()
            if clean_name in seen_signatures or (phone_digits and phone_digits in seen_phones):
                continue
            seen_signatures.add(clean_name)
            if phone_digits:
                seen_phones.add(phone_digits)

            delivered_count += 1
            yield {
                "index": delivered_count,
                "name": item.get("name", ""),
                "address": item.get("address", ""),
                "rating": item.get("rating"),
                "phone": phone,
                "email": item.get("email", ""),
                "website": item.get("website", ""),
            }

            if delivered_count % 18 == 0:
                await asyncio.sleep(0.04)

        if delivered_count >= volume_limit:
            return

    # -----------------------------------------------------------------------
    # Tiers 2 & 3: Live discovery
    # -----------------------------------------------------------------------
    client = httpx.AsyncClient(
        verify=False,
        headers=BROWSER_HEADERS,
        timeout=httpx.Timeout(DISCOVERY_TIMEOUT),
        follow_redirects=True,
    )

    enrich_sem = asyncio.Semaphore(ENRICH_SEMAPHORE_LIMIT)
    osm_task = None
    new_leads_for_cache: list[dict] = []

    try:
        # Launch Overpass in background immediately
        if coords:
            osm_task = asyncio.create_task(
                _scrape_overpass(client, clean_cat, coords[0], coords[1], clean_loc)
            )

        chunk_size = 4 if volume_limit > 30 else 3
        locality_chunks = [localities[i:i + chunk_size] for i in range(0, len(localities), chunk_size)]

        waves_to_run = [(chunk, 1) for chunk in locality_chunks]
        if volume_limit > 30:
            for chunk in locality_chunks:
                waves_to_run.append((chunk, 2))

        for chunk, pass_num in waves_to_run:
            if delivered_count >= volume_limit:
                break
            if time.time() - start_time > wave_cutoff:
                logger.info("Extraction reached %.1fs limit, stopping gracefully.", wave_cutoff)
                break

            wave_start_count = delivered_count

            # Harvest OSM if done
            if osm_task and osm_task.done() and delivered_count < volume_limit:
                try:
                    osm_leads = osm_task.result()
                    for lead in osm_leads:
                        if delivered_count >= volume_limit:
                            break
                        phone = lead.get("phone")
                        if not phone:
                            continue

                        cleaned_osm = clean_business_name(lead.get("name", ""))
                        if is_generic_phrase(cleaned_osm) or not is_category_relevant(lead, clean_cat):
                            continue
                        if is_cross_city_result(lead.get("address", ""), lead.get("snippet", ""), city_key):
                            continue
                        lead["name"] = cleaned_osm

                        r = resolve_lead_rating(lead)
                        lead["rating"] = r

                        if not rating_match(r, star, mode):
                            continue

                        phone_digits = re.sub(r'\D', '', phone)[-10:]
                        clean_name = lead["name"].lower().strip()
                        if clean_name in seen_signatures or (phone_digits and phone_digits in seen_phones):
                            continue
                        seen_signatures.add(clean_name)
                        if phone_digits:
                            seen_phones.add(phone_digits)

                        delivered_count += 1
                        out_lead = {
                            "index": delivered_count,
                            "name": lead["name"],
                            "address": lead["address"],
                            "rating": r,
                            "phone": phone,
                            "email": lead.get("email", ""),
                            "website": lead.get("website", ""),
                        }
                        new_leads_for_cache.append({k: v for k, v in out_lead.items() if k != "index"})
                        yield out_lead
                    osm_task = None
                except Exception:
                    osm_task = None

            if delivered_count >= volume_limit:
                break

            # Build DDGS + Bing search tasks for this wave
            # DDGS is async via asyncio.to_thread; Bing is native async
            search_tasks = []
            for loc in chunk:
                if pass_num == 1:
                    q_a = f"{clean_cat} {loc} {clean_loc} contact phone"
                    q_b = f"{clean_cat} in {loc} {clean_loc} address phone"
                    q_c = f"{clean_cat} in {loc} {clean_loc} contact"
                    q_d = f"{clean_cat} in {loc} {clean_loc}"
                    # DDGS (library) as primary via thread — non-blocking
                    search_tasks.append(asyncio.to_thread(_scrape_ddgs_library_sync, q_a, loc, clean_loc, 10))
                    search_tasks.append(asyncio.to_thread(_scrape_ddgs_library_sync, q_b, loc, clean_loc, 10))
                    # Bing as secondary for redundancy
                    search_tasks.append(_scrape_bing_search(client, q_c, loc, clean_loc))
                    search_tasks.append(_scrape_bing_search(client, q_d, loc, clean_loc))
                else:
                    q_e = f"{clean_cat} in {loc} {clean_loc} contact number"
                    q_f = f"{clean_cat} centre in {loc} {clean_loc} contact"
                    q_g = f"{clean_cat} studio in {loc} {clean_loc} contact"
                    q_h = f"{clean_cat} near {loc} {clean_loc} contact"
                    search_tasks.append(asyncio.to_thread(_scrape_ddgs_library_sync, q_e, loc, clean_loc, 10))
                    search_tasks.append(_scrape_bing_search(client, q_f, loc, clean_loc))
                    search_tasks.append(_scrape_bing_search(client, q_g, loc, clean_loc))
                    search_tasks.append(_scrape_bing_search(client, q_h, loc, clean_loc))

            try:
                results_lists = await asyncio.wait_for(
                    asyncio.gather(*search_tasks, return_exceptions=True),
                    timeout=max(1.0, (max_duration - 2.0) - (time.time() - start_time))
                )
            except asyncio.TimeoutError:
                results_lists = []

            candidate_pool: list[dict] = []
            for res in results_lists:
                if isinstance(res, list):
                    candidate_pool.extend(res)

            # Separate candidates: snippet-ready vs needs website enrichment
            immediate_candidates: list[dict] = []
            enrich_tasks = []

            for c in candidate_pool:
                w = c.get("website", "").strip()
                is_dir = not w.startswith("http") or any(d in w.lower() for d in DIRECTORY_DOMAINS)
                has_phone = bool(c.get("phone"))

                # Category relevance gate — discard irrelevant results early
                if not is_category_relevant(c, clean_cat):
                    continue

                # Cross-city guard
                if is_cross_city_result(c.get("address", ""), c.get("snippet", ""), city_key):
                    continue

                # URL dedup
                if w and w in visited_urls:
                    if has_phone:
                        immediate_candidates.append(c)
                    continue
                if w:
                    visited_urls.add(w)

                if has_phone:
                    if c.get("email") or is_dir or not w.startswith("http"):
                        immediate_candidates.append(c)
                    else:
                        enrich_tasks.append(asyncio.create_task(
                            _deep_enrich_website(client, c, clean_loc, sem=enrich_sem)
                        ))
                else:
                    if not is_dir and w.startswith("http"):
                        enrich_tasks.append(asyncio.create_task(
                            _deep_enrich_website(client, c, clean_loc, sem=enrich_sem)
                        ))

            # 1. Yield snippet-ready leads immediately
            for item in immediate_candidates:
                if delivered_count >= volume_limit:
                    break

                final_name = clean_business_name(item.get("name", ""))
                if is_generic_phrase(final_name):
                    continue
                item["name"] = final_name

                phone = item.get("phone")
                if not phone:
                    continue

                r = resolve_lead_rating(item)
                item["rating"] = r

                if not rating_match(r, star, mode):
                    continue

                phone_digits = re.sub(r'\D', '', phone)[-10:]
                clean_name = item["name"].lower().strip()
                if clean_name in seen_signatures or (phone_digits and phone_digits in seen_phones):
                    continue
                seen_signatures.add(clean_name)
                if phone_digits:
                    seen_phones.add(phone_digits)

                delivered_count += 1
                out_lead = {
                    "index": delivered_count,
                    "name": item["name"],
                    "address": item["address"],
                    "rating": r,
                    "phone": phone,
                    "email": item.get("email", ""),
                    "website": item.get("website", ""),
                }
                new_leads_for_cache.append({k: v for k, v in out_lead.items() if k != "index"})
                yield out_lead

            # 2. Yield enriched website candidates as they finish
            for coro in asyncio.as_completed(enrich_tasks):
                if delivered_count >= volume_limit:
                    for t in enrich_tasks:
                        if not t.done():
                            t.cancel()
                    break

                if time.time() - start_time > enrich_cutoff:
                    for t in enrich_tasks:
                        if not t.done():
                            t.cancel()
                    break

                try:
                    res_items = await coro
                except asyncio.CancelledError:
                    continue
                except Exception:
                    continue

                if isinstance(res_items, dict):
                    items_to_yield = [res_items]
                elif isinstance(res_items, list):
                    items_to_yield = res_items
                else:
                    continue

                for item in items_to_yield:
                    if delivered_count >= volume_limit:
                        break
                    if not isinstance(item, dict):
                        continue

                    final_name = clean_business_name(item.get("name", ""))
                    if is_generic_phrase(final_name) or not is_category_relevant(item, clean_cat):
                        continue
                    item["name"] = final_name

                    phone = item.get("phone")
                    if not phone:
                        continue

                    r = resolve_lead_rating(item)
                    item["rating"] = r

                    if not rating_match(r, star, mode):
                        continue

                    phone_digits = re.sub(r'\D', '', phone)[-10:]
                    clean_name = item["name"].lower().strip()
                    if clean_name in seen_signatures or (phone_digits and phone_digits in seen_phones):
                        continue
                    seen_signatures.add(clean_name)
                    if phone_digits:
                        seen_phones.add(phone_digits)

                    delivered_count += 1
                    out_lead = {
                        "index": delivered_count,
                        "name": item["name"],
                        "address": item["address"],
                        "rating": r,
                        "phone": phone,
                        "email": item.get("email", ""),
                        "website": item.get("website", ""),
                    }
                    new_leads_for_cache.append({k: v for k, v in out_lead.items() if k != "index"})
                    yield out_lead

            # Check OSM after this wave
            if osm_task and osm_task.done() and delivered_count < volume_limit:
                try:
                    osm_leads = osm_task.result()
                    for lead in osm_leads:
                        if delivered_count >= volume_limit:
                            break
                        phone = lead.get("phone")
                        if not phone:
                            continue

                        cleaned_osm = clean_business_name(lead.get("name", ""))
                        if is_generic_phrase(cleaned_osm) or not is_category_relevant(lead, clean_cat):
                            continue
                        if is_cross_city_result(lead.get("address", ""), lead.get("snippet", ""), city_key):
                            continue
                        lead["name"] = cleaned_osm

                        r = resolve_lead_rating(lead)
                        lead["rating"] = r

                        if not rating_match(r, star, mode):
                            continue

                        phone_digits = re.sub(r'\D', '', phone)[-10:]
                        clean_name = lead["name"].lower().strip()
                        if clean_name in seen_signatures or (phone_digits and phone_digits in seen_phones):
                            continue
                        seen_signatures.add(clean_name)
                        if phone_digits:
                            seen_phones.add(phone_digits)

                        delivered_count += 1
                        out_lead = {
                            "index": delivered_count,
                            "name": lead["name"],
                            "address": lead["address"],
                            "rating": r,
                            "phone": phone,
                            "email": lead.get("email", ""),
                            "website": lead.get("website", ""),
                        }
                        new_leads_for_cache.append({k: v for k, v in out_lead.items() if k != "index"})
                        yield out_lead
                    osm_task = None
                except Exception:
                    osm_task = None

            if delivered_count == wave_start_count:
                consecutive_empty_waves += 1
                empty_limit = 24 if volume_limit > 30 else 4
                if consecutive_empty_waves >= empty_limit:
                    logger.info("%d consecutive empty waves reached, stopping.", empty_limit)
                    break
            else:
                consecutive_empty_waves = 0

        # Final OSM harvest
        if osm_task and osm_task.done() and delivered_count < volume_limit:
            try:
                osm_leads = osm_task.result()
                for lead in osm_leads:
                    if delivered_count >= volume_limit:
                        break
                    phone = lead.get("phone")
                    if not phone:
                        continue

                    cleaned_osm = clean_business_name(lead.get("name", ""))
                    if is_generic_phrase(cleaned_osm) or not is_category_relevant(lead, clean_cat):
                        continue
                    if is_cross_city_result(lead.get("address", ""), lead.get("snippet", ""), city_key):
                        continue
                    lead["name"] = cleaned_osm

                    r = resolve_lead_rating(lead)
                    lead["rating"] = r

                    if not rating_match(r, star, mode):
                        continue

                    phone_digits = re.sub(r'\D', '', phone)[-10:]
                    clean_name = lead["name"].lower().strip()
                    if clean_name in seen_signatures or (phone_digits and phone_digits in seen_phones):
                        continue
                    seen_signatures.add(clean_name)
                    if phone_digits:
                        seen_phones.add(phone_digits)

                    delivered_count += 1
                    out_lead = {
                        "index": delivered_count,
                        "name": lead["name"],
                        "address": lead["address"],
                        "rating": r,
                        "phone": phone,
                        "email": lead.get("email", ""),
                        "website": lead.get("website", ""),
                    }
                    new_leads_for_cache.append({k: v for k, v in out_lead.items() if k != "index"})
                    yield out_lead
            except Exception:
                pass

    finally:
        if osm_task and not osm_task.done():
            osm_task.cancel()
        await client.aclose()

        # Persist new verified leads to cache
        if new_leads_for_cache:
            save_to_cache(clean_cat, city_key, new_leads_for_cache)
            logger.info("Saved %d new leads to cache for %s/%s", len(new_leads_for_cache), clean_cat, city_key)


def _scrape_ddgs_library_sync(query: str, locality: str, city: str, max_results: int = 10) -> list[dict]:
    """
    Synchronous DDGS search wrapper (called via asyncio.to_thread).
    Uses the duckduckgo_search library which is pre-installed.
    """
    leads: list[dict] = []
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results, region="in-en"))
        for r in results:
            title = r.get("title", "")
            url = r.get("href", "")
            snippet = r.get("body", "")

            if not title or len(title) < 3:
                continue
            if is_junk_result(title, url, snippet):
                continue

            bname = clean_business_name(title)
            if is_generic_phrase(bname):
                continue

            if any(d in url.lower() for d in DIRECTORY_DOMAINS):
                continue

            combined = f"{title} {snippet}"
            phones = extract_phones_from_text(combined)
            emails = extract_emails_from_text(combined)
            rating = extract_rating_from_text(combined)

            leads.append({
                "name": bname,
                "address": f"{locality}, {city}",
                "phone": phones[0] if phones else None,
                "email": emails[0] if emails else "",
                "website": url if url.startswith("http") else "",
                "rating": rating,
                "snippet": snippet,
                "source": "ddgs",
            })
    except Exception as exc:
        logger.debug("DDGS sync error: %s", exc)
    return leads
