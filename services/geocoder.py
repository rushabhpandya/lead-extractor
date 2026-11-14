"""
services/geocoder.py — Global sub-district fan-out maps + Nominatim dynamic resolution.
Supports Indian cities (preserved), plus global capitals for luxury B2B lead extraction.
"""

from __future__ import annotations
import httpx
import logging
from typing import List, Tuple

logger = logging.getLogger("geocoder")

# ---------------------------------------------------------------------------
# City → (ISO country code, list of sub-localities to fan out)
# ---------------------------------------------------------------------------

CITY_SUBLOCALITY_MAP: dict[str, Tuple[str, List[str]]] = {
    # ── INDIA (preserved from original scraper) ──────────────────────────
    "mumbai":    ("IN", ["Andheri", "Bandra", "Borivali", "Dadar", "Juhu", "Kurla",
                          "Malad", "Powai", "Thane", "Vile Parle", "Worli", "Chembur",
                          "Goregaon", "Kandivali", "Mulund", "Versova"]),
    "delhi":     ("IN", ["Connaught Place", "Dwarka", "Janakpuri", "Karol Bagh",
                          "Lajpat Nagar", "Nehru Place", "Rohini", "Saket",
                          "South Extension", "Vasant Kunj", "Pitampura", "Mayur Vihar"]),
    "bengaluru": ("IN", ["Indiranagar", "Jayanagar", "JP Nagar", "Koramangala",
                          "Malleshwaram", "MG Road", "Rajajinagar", "Whitefield",
                          "Electronic City", "Hebbal", "HSR Layout", "BTM Layout"]),
    "bangalore": ("IN", ["Indiranagar", "Koramangala", "MG Road", "Whitefield",
                          "Jayanagar", "HSR Layout", "BTM Layout", "Electronic City"]),
    "pune":      ("IN", ["Aundh", "Baner", "FC Road", "Hadapsar", "Hinjewadi",
                          "Kharadi", "Koregaon Park", "Magarpatta", "Pimpri",
                          "Shivajinagar", "Viman Nagar", "Wakad"]),
    "hyderabad": ("IN", ["Banjara Hills", "Gachibowli", "Hitech City", "Jubilee Hills",
                          "Kondapur", "Kukatpally", "Madhapur", "Secunderabad",
                          "Tolichowki", "Uppal", "Miyapur"]),
    "chennai":   ("IN", ["Adyar", "Anna Nagar", "Besant Nagar", "Chromepet", "Egmore",
                          "Mylapore", "Nungambakkam", "Tambaram", "T Nagar",
                          "Velachery", "OMR", "Porur"]),
    "kolkata":   ("IN", ["Ballygunge", "Behala", "Dum Dum", "Jadavpur", "Lake Town",
                          "New Town", "Park Street", "Salt Lake", "Tollygunge"]),
    "ahmedabad": ("IN", ["Bopal", "CG Road", "Gota", "Maninagar", "Navrangpura",
                          "Paldi", "Prahlad Nagar", "SG Highway", "Thaltej"]),
    "jaipur":    ("IN", ["Bani Park", "C-Scheme", "Jagatpura", "Malviya Nagar",
                          "Mansarovar", "Sanganer", "Vaishali Nagar"]),
    "surat":     ("IN", ["Adajan", "Athwa", "Katargam", "Piplod", "Rander", "Varachha"]),
    "lucknow":   ("IN", ["Alambagh", "Aliganj", "Gomti Nagar", "Hazratganj",
                          "Indira Nagar", "Mahanagar"]),
    "nagpur":    ("IN", ["Civil Lines", "Dharampeth", "Gandhibagh", "Hingna",
                          "Manish Nagar", "Sitabuldi"]),
    "patna":     ("IN", ["Ashok Rajpath", "Bailey Road", "Boring Road", "Frazer Road",
                          "Kankarbagh"]),
    "bhopal":    ("IN", ["Arera Colony", "Bawadia Kalan", "Kolar", "MP Nagar", "Shahpura"]),
    "indore":    ("IN", ["AB Road", "Palasia", "Rajwada", "Scheme 54", "Vijay Nagar"]),
    "visakhapatnam": ("IN", ["Dwaraka Nagar", "Gajuwaka", "Maddilapalem", "MVP Colony",
                               "Seethammadhara"]),
    "vadodara":  ("IN", ["Alkapuri", "Fatehganj", "Gorwa", "Karelibaug", "Manjalpur"]),
    "coimbatore": ("IN", ["Gandhipuram", "Peelamedu", "RS Puram", "Saibaba Colony",
                            "Singanallur"]),
    "kochi":     ("IN", ["Edapally", "Fort Kochi", "Kakkanad", "MG Road",
                          "Palarivattom", "Vyttila"]),
    "goa":       ("IN", ["Calangute", "Mapusa", "Margao", "Panaji", "Vasco da Gama"]),

    # ── UNITED KINGDOM ───────────────────────────────────────────────────
    "london":    ("GB", ["Mayfair", "Belgravia", "Chelsea", "Kensington",
                          "Knightsbridge", "Notting Hill", "Marylebone",
                          "St John's Wood", "Pimlico", "Fulham", "Islington",
                          "Shoreditch", "Fitzrovia"]),
    "mayfair, london": ("GB", ["Mayfair", "Mount Street", "New Bond Street",
                                 "Old Bond Street", "Dover Street", "Berkeley Square",
                                 "Grosvenor Square", "Bruton Street"]),
    "mayfair":   ("GB", ["Mayfair", "Mount Street", "New Bond Street",
                          "Old Bond Street", "Dover Street", "Berkeley Square"]),

    # ── UNITED STATES ────────────────────────────────────────────────────
    "new york":  ("US", ["Manhattan", "Upper East Side", "SoHo", "Chelsea",
                          "Tribeca", "Midtown", "Upper West Side", "Greenwich Village",
                          "Lower East Side", "Brooklyn Heights", "Williamsburg"]),
    "new york, usa": ("US", ["Manhattan", "Upper East Side", "SoHo", "Chelsea",
                               "Tribeca", "Midtown"]),
    "manhattan, ny": ("US", ["Upper East Side", "SoHo", "Chelsea", "Tribeca",
                               "Midtown East", "Midtown West", "Upper West Side",
                               "Flatiron", "NoHo", "TriBeCa"]),
    "manhattan, new york": ("US", ["Upper East Side", "SoHo", "Chelsea",
                                     "Midtown East", "Upper West Side", "Flatiron"]),
    "manhattan": ("US", ["Upper East Side", "SoHo", "Chelsea", "Tribeca",
                          "Midtown East", "Upper West Side"]),
    "los angeles": ("US", ["Beverly Hills", "West Hollywood", "Silver Lake",
                             "Studio City", "Brentwood", "Santa Monica",
                             "Culver City", "Echo Park", "Los Feliz"]),
    "los angeles, usa": ("US", ["Beverly Hills", "West Hollywood", "Brentwood",
                                  "Santa Monica", "Studio City"]),
    "palm beach": ("US", ["Palm Beach", "Worth Avenue", "Royal Palm Way",
                            "South Ocean Boulevard", "North County Road"]),
    "palm beach, usa": ("US", ["Palm Beach", "Worth Avenue", "Royal Palm Way"]),
    "miami":     ("US", ["Brickell", "Coconut Grove", "Coral Gables", "Design District",
                          "South Beach", "Wynwood", "Bal Harbour"]),
    "san francisco": ("US", ["SOMA", "Nob Hill", "Pacific Heights", "Mission District",
                               "Hayes Valley", "Castro"]),

    # ── FRANCE ───────────────────────────────────────────────────────────
    "paris":     ("FR", ["1er", "6ème", "7ème", "8ème", "16ème",
                          "Marais", "Saint-Germain-des-Prés", "Rue du Bac",
                          "Saint-Ouen", "Carré Rive Gauche", "Triangle d'Or"]),
    "paris, france": ("FR", ["Marais", "Saint-Germain-des-Prés", "Rue du Bac",
                               "Saint-Ouen", "Triangle d'Or", "8ème"]),

    # ── UAE ──────────────────────────────────────────────────────────────
    "dubai":     ("AE", ["Downtown Dubai", "DIFC", "Jumeirah", "Al Quoz",
                          "Dubai Marina", "Business Bay", "Palm Jumeirah",
                          "Deira", "Bur Dubai", "JLT"]),
    "dubai, uae": ("AE", ["Downtown Dubai", "DIFC", "Jumeirah", "Al Quoz",
                            "Dubai Marina", "Palm Jumeirah"]),
    "abu dhabi": ("AE", ["Corniche", "Saadiyat Island", "Yas Island", "Al Reem Island"]),

    # ── OTHER GLOBAL CITIES ───────────────────────────────────────────────
    "amsterdam": ("NL", ["Jordaan", "De Pijp", "Centrum", "Oud-West", "Zuidas"]),
    "berlin":    ("DE", ["Mitte", "Prenzlauer Berg", "Charlottenburg", "Kreuzberg",
                          "Friedrichshain"]),
    "milan":     ("IT", ["Brera", "Quadrilatero della Moda", "Navigli", "Porta Venezia",
                          "CityLife"]),
    "hong kong": ("HK", ["Central", "Wan Chai", "Tsim Sha Tsui", "Admiralty",
                          "Sheung Wan"]),
    "singapore": ("SG", ["Orchard", "Marina Bay", "Dempsey Hill", "Holland Village",
                          "Bugis"]),
    "sydney":    ("AU", ["CBD", "Paddington", "Surry Hills", "Newtown", "Bondi",
                          "Darlinghurst"]),
    "toronto":   ("CA", ["Yorkville", "Distillery District", "King West", "Annex",
                          "Kensington Market"]),
}

# Country ISO → country name (for display/enrichment)
ISO_COUNTRY_NAMES: dict[str, str] = {
    "IN": "India", "GB": "United Kingdom", "US": "United States",
    "FR": "France", "AE": "United Arab Emirates", "NL": "Netherlands",
    "DE": "Germany", "IT": "Italy", "HK": "Hong Kong", "SG": "Singapore",
    "AU": "Australia", "CA": "Canada",
}

# Country ISO → default phone region for phonenumbers library
ISO_PHONE_REGION: dict[str, str] = {
    "IN": "IN", "GB": "GB", "US": "US", "FR": "FR",
    "AE": "AE", "NL": "NL", "DE": "DE", "IT": "IT",
    "HK": "HK", "SG": "SG", "AU": "AU", "CA": "CA",
}


def get_country_and_sublocalities(city_input: str) -> tuple[str, list[str]]:
    """
    Return (iso_country_code, list_of_sublocalities) for a given city input.
    Falls back to Nominatim for unknown cities.
    """
    key = city_input.strip().lower()

    # Direct lookup
    if key in CITY_SUBLOCALITY_MAP:
        iso, subs = CITY_SUBLOCALITY_MAP[key]
        return iso, subs

    # Partial match — first word or city name appears in key
    for map_key, (iso, subs) in CITY_SUBLOCALITY_MAP.items():
        if map_key in key or key.split(",")[0].strip() in map_key:
            return iso, subs

    # Dynamic Nominatim fallback (sync wrapper — called rarely)
    return _nominatim_resolve(city_input)


def _nominatim_resolve(city_input: str) -> tuple[str, list[str]]:
    """
    Call Nominatim to get country ISO and return generic sub-localities.
    Returns ("", [city_input]) on any failure so scraping still works.
    """
    try:
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": city_input, "format": "json", "limit": 1, "addressdetails": 1},
            headers={"User-Agent": "BigLeaguesB2B/1.0 contact@bigleagues.agency"},
            timeout=4.0,
        )
        data = resp.json()
        if data:
            address = data[0].get("address", {})
            iso = address.get("country_code", "").upper() or "GB"
            return iso, [city_input]
    except Exception as exc:
        logger.warning("Nominatim resolve failed for %s: %s", city_input, exc)
    return "GB", [city_input]


def parse_multi_city(location_input: str) -> list[str]:
    """
    Parse a semicolon- or pipe-separated list of cities.
    e.g. "Mayfair, London; Manhattan, NY; Paris" → 3 cities.
    """
    import re
    cities = [c.strip() for c in re.split(r"[;|]", location_input) if c.strip()]
    return cities if cities else [location_input.strip()]
