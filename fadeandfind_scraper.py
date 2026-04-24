"""
Fade & Find — Scraper v2
=========================
Pulls estate sales from EstateSales.NET, auctions from AuctionZip,
and permanent stores from Google Places API.
Pushes directly to Supabase via REST API.

Runs non-interactively via GitHub Actions.
All secrets come from environment variables.

Usage:
  python fadeandfind_scraper.py [--mode daily|weekly|full]
  
  daily  = estate sales + auctions only (fast, runs every day)
  weekly = adds Google Places refresh (slower, runs weekly)
  full   = everything, all metros (used for initial population)
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import math
import os
import sys
import json
from datetime import datetime, timezone

# ── Config from environment ────────────────────────────────
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "https://dmriqhvmsfxujwiizigg.supabase.co")
SUPABASE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY", "")
GOOGLE_API_KEY   = os.environ.get("GOOGLE_API_KEY", "")

MODE = "daily"
if len(sys.argv) > 1 and sys.argv[1].startswith("--mode="):
    MODE = sys.argv[1].split("=")[1]
elif len(sys.argv) > 2 and sys.argv[1] == "--mode":
    MODE = sys.argv[2]

print(f"Mode: {MODE}")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

SALE_LINK_PATTERN = re.compile(r'^/([A-Z]{2})/([^/]+)/(\d{5})/(\d+)$')
LISTING_PATTERN   = re.compile(r'/listings/(\d+)\.html', re.IGNORECASE)

# ── US Metro areas to scrape ───────────────────────────────
# Format: (state, city_url, zip, radius_miles)
US_METROS = [
    # Midwest
    ("NE", "Blair",         "68008", 150),
    ("NE", "Omaha",         "68102", 100),
    ("IA", "Des-Moines",    "50309", 100),
    ("MO", "Kansas-City",   "64101", 100),
    ("MO", "St-Louis",      "63101", 100),
    ("MN", "Minneapolis",   "55401", 100),
    ("WI", "Milwaukee",     "53202", 100),
    ("IL", "Chicago",       "60601", 100),
    ("IN", "Indianapolis",  "46204", 100),
    ("OH", "Columbus",      "43215", 100),
    ("OH", "Cleveland",     "44114", 100),
    ("MI", "Detroit",       "48226", 100),
    ("KS", "Wichita",       "67202", 100),
    ("OK", "Oklahoma-City", "73102", 100),
    # South
    ("TX", "Dallas",        "75201", 100),
    ("TX", "Houston",       "77002", 100),
    ("TX", "San-Antonio",   "78205", 100),
    ("TX", "Austin",        "78701", 100),
    ("FL", "Orlando",       "32801", 100),
    ("FL", "Tampa",         "33602", 100),
    ("FL", "Miami",         "33131", 100),
    ("FL", "Jacksonville",  "32202", 100),
    ("GA", "Atlanta",       "30303", 100),
    ("NC", "Charlotte",     "28202", 100),
    ("NC", "Raleigh",       "27601", 100),
    ("TN", "Nashville",     "37201", 100),
    ("TN", "Memphis",       "38103", 100),
    ("AL", "Birmingham",    "35203", 100),
    ("LA", "New-Orleans",   "70112", 100),
    ("SC", "Columbia",      "29201", 100),
    ("VA", "Richmond",      "23219", 100),
    # Northeast
    ("NY", "New-York",      "10001", 75),
    ("NY", "Buffalo",       "14202", 100),
    ("PA", "Philadelphia",  "19103", 100),
    ("PA", "Pittsburgh",    "15222", 100),
    ("MA", "Boston",        "02101", 100),
    ("CT", "Hartford",      "06103", 100),
    ("NJ", "Newark",        "07102", 75),
    ("MD", "Baltimore",     "21201", 100),
    ("DC", "Washington",    "20001", 100),
    # West
    ("CA", "Los-Angeles",   "90001", 75),
    ("CA", "San-Francisco", "94102", 75),
    ("CA", "San-Diego",     "92101", 100),
    ("CA", "Sacramento",    "95814", 100),
    ("WA", "Seattle",       "98101", 100),
    ("OR", "Portland",      "97201", 100),
    ("AZ", "Phoenix",       "85001", 100),
    ("AZ", "Tucson",        "85701", 100),
    ("NV", "Las-Vegas",     "89101", 100),
    ("CO", "Denver",        "80202", 100),
    ("UT", "Salt-Lake-City","84101", 100),
    ("NM", "Albuquerque",   "87101", 100),
    ("ID", "Boise",         "83702", 100),
    ("MT", "Billings",      "59101", 150),
    ("WY", "Cheyenne",      "82001", 150),
    ("SD", "Sioux-Falls",   "57104", 150),
    ("ND", "Fargo",         "58102", 150),
    ("AR", "Little-Rock",   "72201", 100),
    ("MS", "Jackson",       "39201", 100),
    ("KY", "Louisville",    "40202", 100),
    ("WV", "Charleston",    "25301", 100),
]

# Daily mode: just scrape the core Midwest area (fast)
DAILY_METROS = [
    ("NE", "Blair",         "68008", 150),
    ("NE", "Omaha",         "68102", 100),
    ("IA", "Des-Moines",    "50309", 100),
    ("MO", "Kansas-City",   "64101", 100),
    ("MO", "St-Louis",      "63101", 100),
    ("MN", "Minneapolis",   "55401", 100),
    ("IL", "Chicago",       "60601", 100),
    ("TX", "Dallas",        "75201", 100),
    ("TX", "Houston",       "77002", 100),
    ("FL", "Orlando",       "32801", 100),
    ("GA", "Atlanta",       "30303", 100),
    ("NY", "New-York",      "10001", 75),
    ("CA", "Los-Angeles",   "90001", 75),
    ("WA", "Seattle",       "98101", 100),
    ("CO", "Denver",        "80202", 100),
]

PLACES_SEARCHES = [
    ("antique store",    "antique"),
    ("antique mall",     "antique"),
    ("thrift store",     "thrift"),
    ("flea market",      "flea"),
    ("consignment shop", "thrift"),
    ("vintage shop",     "antique"),
]


# ══════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════

def haversine_miles(lat1, lng1, lat2, lng2):
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 1)


def geocode(query):
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1},
            headers={"User-Agent": "FadeAndFind/2.0 (estate-sale-finder)"},
            timeout=10,
        )
        if resp.status_code == 200 and resp.json():
            r = resp.json()[0]
            return float(r["lat"]), float(r["lon"])
    except Exception:
        pass
    return None, None


# ══════════════════════════════════════════════════════════
# ESTATESALES.NET SCRAPER
# ══════════════════════════════════════════════════════════

def clean_name(raw_name):
    name = re.sub(r'^\d+', '', raw_name).strip()
    name = re.sub(r'^(?:Nationally\s+Featured|Staff\s+Pick|Featured|New\s+Sale)\s*', '', name, flags=re.IGNORECASE).strip()
    cut_patterns = [
        r'Listed\s*by\b', r'Privately\s+Listed', r'Last\s+modified',
        r'\d+\s*(?:Pictures?|Photos?)', r'\d{5}', r'\d+\s*miles',
        r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d',
        r'\d{1,2}(?::\d{2})?\s*(?:am|pm)',
    ]
    for pat in cut_patterns:
        m = re.search(pat, name, re.IGNORECASE)
        if m:
            name = name[:m.start()].strip()
    name = re.sub(r'[\s\-|]+$', '', name).strip()
    return name if name else raw_name[:80]


def parse_container(container_text):
    fields = {"address": "", "distance": "", "photo_count": 0, "dates": "", "times": "", "company": "", "status": ""}
    photo_match = re.search(r'(\d+)\s*(?:Pictures?|Photos?)', container_text, re.IGNORECASE)
    if photo_match:
        fields["photo_count"] = int(photo_match.group(1))
    dist_match = re.search(r'(\d+(?:\.\d+)?)\s*miles?\s*away', container_text, re.IGNORECASE)
    if dist_match:
        fields["distance"] = f"{dist_match.group(1)} miles"
    addr_match = re.search(
        r'(\d+\s+(?:[NSEW]\.?\s+)?[A-Za-z0-9][A-Za-z0-9\s\.\-]*?'
        r'(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Way|Court|Ct|'
        r'Place|Pl|Terrace|Ter|Trail|Trl|Circle|Cir|Pike|Hwy|Highway|'
        r'\d+(?:st|nd|rd|th)\s+(?:Street|St|Ave|Avenue|Terrace|Ter|Road|Rd|Drive|Dr|Court|Ct|Place|Pl|Blvd|Lane|Ln))\.?)',
        container_text, re.IGNORECASE
    )
    if addr_match:
        fields["address"] = addr_match.group(1).strip()
    date_match = re.search(
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}'
        r'(?:[,\s]+\d{1,2})*(?:\s+to\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2})?)',
        container_text, re.IGNORECASE
    )
    if date_match:
        raw = date_match.group(1).strip().rstrip(",")
        raw = re.sub(r',\s+(\d)', r', \1', raw)
        fields["dates"] = raw
    time_match = re.search(
        r'(\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*(?:to|-)\s*\d{1,2}(?::\d{2})?\s*(?:am|pm))',
        container_text, re.IGNORECASE
    )
    if time_match:
        fields["times"] = time_match.group(1).strip()
    company_match = re.search(
        r'Listed\s*by\s+(.+?)(?=\s*Last\s+modified|\s*\d+\s*(?:Pictures?|Photos?)|\||$)',
        container_text, re.IGNORECASE
    )
    if company_match:
        fields["company"] = company_match.group(1).strip()[:80]
    for tag in ["Ends Today!", "Going on Now!", "Starts at", "Coming Soon"]:
        if tag.lower() in container_text.lower():
            fields["status"] = tag
            break
    return fields


def scrape_estatesales_detail(url, session=None):
    """
    Fetch individual estate sale listing page.
    Returns (street_address, dates_string) — both may be empty strings on failure.
    Extracts dates from the detail page since the grid page rarely exposes them.
    """
    try:
        requester = session or requests
        resp = requester.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return "", ""
        soup = BeautifulSoup(resp.text, "html.parser")
        street = ""
        dates  = ""

        # ── Address ──────────────────────────────────────────────────────────

        # Method 1: schema.org JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    data = data[0]
                addr = data.get("location", {}).get("address", {}) or data.get("address", {})
                s = addr.get("streetAddress", "")
                if s:
                    street = s.strip()
                    break
            except Exception:
                pass

        # Method 2: HTML selectors
        if not street:
            for selector in [
                "[itemprop='streetAddress']",
                "[class*='address']",
                "[class*='location']",
                ".sale-address",
                "#sale-address",
            ]:
                el = soup.select_one(selector)
                if el:
                    t = el.get_text(strip=True)
                    if re.match(r'^\d+\s+', t):
                        street = t.strip()
                        break

        # Method 3: regex scan full page text
        if not street:
            page_text = soup.get_text(separator=" ", strip=True)
            m = re.search(
                r'(\d+\s+(?:[NSEW]\.?\s+)?[A-Za-z0-9][A-Za-z0-9\s\.\-]*?'
                r'(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Way|'
                r'Court|Ct|Place|Pl|Terrace|Ter|Trail|Trl|Circle|Cir|'
                r'\d+(?:st|nd|rd|th)\s+(?:St|Ave|Rd|Dr|Ct|Pl|Blvd|Ln))\.?)',
                page_text, re.IGNORECASE
            )
            if m:
                street = m.group(1).strip()

        # ── Dates ────────────────────────────────────────────────────────────
        page_text = soup.get_text(separator=" ", strip=True)

        # Method 1: JSON-LD startDate / endDate (most reliable)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    data = data[0]
                start = data.get("startDate", "")
                end   = data.get("endDate", "")
                # Use end date for expiry; fall back to start if no end
                date_str = end if end else start
                if date_str:
                    try:
                        d = datetime.fromisoformat(date_str[:10])
                        dates = d.strftime("%b %d")
                    except Exception:
                        dates = date_str[:10]
                    break
            except Exception:
                pass

        # Method 2: visible date elements (EstateSales.NET renders day boxes)
        if not dates:
            # Look for date boxes like "Apr 18" / "Apr 19"
            date_els = soup.select("[class*='date'], [class*='day'], time")
            found_dates = []
            for el in date_els:
                t = el.get_text(strip=True)
                dm = re.search(
                    r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2})',
                    t, re.IGNORECASE
                )
                if dm:
                    found_dates.append(dm.group(1).strip())
            if found_dates:
                # Store as "Apr 18 to Apr 19" or just "Apr 18"
                unique = list(dict.fromkeys(found_dates))  # dedupe, preserve order
                dates = " to ".join(unique) if len(unique) > 1 else unique[0]

        # Method 3: regex on full page text
        if not dates:
            date_m = re.search(
                r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}'
                r'(?:[,\s]+\d{1,2})*'
                r'(?:\s*(?:to|-)\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2})?)',
                page_text, re.IGNORECASE
            )
            if date_m:
                dates = date_m.group(1).strip().rstrip(",")

    except Exception:
        pass

    return street, dates


def scrape_estatesales(state, city, zip_code):
    url = f"https://www.estatesales.net/{state}/{city}/{zip_code}"
    print(f"  🌐 EstateSales: {url}")
    listings = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            print(f"     ⚠️  Status {resp.status_code}")
            return listings
        soup = BeautifulSoup(resp.text, "html.parser")
        sale_links = []
        seen_ids = set()
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            match = SALE_LINK_PATTERN.match(href)
            if match:
                sale_id = match.group(4)
                if sale_id not in seen_ids:
                    seen_ids.add(sale_id)
                    sale_links.append({"element": a_tag, "href": href,
                                       "sale_state": match.group(1), "sale_city": match.group(2),
                                       "sale_zip": match.group(3), "sale_id": sale_id})
        print(f"     Found {len(sale_links)} listings")
        for link in sale_links:
            a_tag   = link["element"]
            sale_id = link["sale_id"]
            raw_name = a_tag.get_text(separator=" ", strip=True)
            name = clean_name(raw_name)
            detail_url = f"https://www.estatesales.net{link['href']}"
            listing = {
                "id":         f"es_{sale_id}",
                "name":       name,
                "city":       link["sale_city"].replace("-", " ").title(),
                "state":      link["sale_state"],
                "zip":        link["sale_zip"],
                "url":        detail_url,
                "lat":        None, "lng": None,
                "address":    "", "distance": "", "photo_count": 0,
                "dates":      "", "times": "", "company": "",
                "status":     "", "category": "estate",
                "source":     "estatesales.net",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            container = a_tag
            container_text = ""
            for _ in range(12):
                if container.parent:
                    container = container.parent
                    text = container.get_text(separator=" | ", strip=True)
                    if bool(re.search(r'\d{5}', text)) and bool(re.search(r'miles?\s*away|Pictures?|Listed\s*by', text, re.IGNORECASE)) and len(text) > 100:
                        container_text = text
                        break
            if not container_text:
                container_text = a_tag.parent.get_text(separator=" | ", strip=True) if a_tag.parent else ""
            parsed = parse_container(container_text)
            listing.update(parsed)

            # Fetch detail page if address OR dates are missing
            # Both are extracted in one request — no extra cost
            needs_address = not listing.get("address") or not re.match(r'^\d+\s+', listing.get("address", ""))
            needs_dates   = not listing.get("dates")
            if needs_address or needs_dates:
                print(f"     🔍 Fetching detail for: {name[:45]}")
                street, dates = scrape_estatesales_detail(detail_url)
                if street:
                    listing["address"] = street
                    print(f"         ✓ Address: {street}")
                if dates:
                    listing["dates"] = dates
                    print(f"         ✓ Dates:   {dates}")
                time.sleep(1.5)  # be polite to estatesales.net

            if "auction" in container_text.lower() or "auction" in name.lower():
                listing["category"] = "auction"
            listings.append(listing)
    except Exception as e:
        print(f"     ❌ Error: {e}")
    return listings


# ══════════════════════════════════════════════════════════
# AUCTIONZIP SCRAPER
# ══════════════════════════════════════════════════════════

AUCTIONZIP_STATES = [
    "al","ak","az","ar","ca","co","ct","de","fl","ga",
    "hi","id","il","in","ia","ks","ky","la","me","md",
    "ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc",
    "sd","tn","tx","ut","vt","va","wa","wv","wi","wy"
]

def scrape_auctionzip(target_states, origin_lat, origin_lng, max_distance):
    listings = []
    print(f"  🔨 AuctionZip: {', '.join(s.upper() for s in target_states)}")
    for state in target_states:
        url = f"https://www.auctionzip.com/{state}.html"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                print(f"     ⚠️  {state.upper()} returned {resp.status_code}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            found = 0
            for a in soup.find_all("a", href=True):
                m = LISTING_PATTERN.search(a["href"])
                if not m:
                    continue
                lid = m.group(1)
                if any(l["id"] == f"az_{lid}" for l in listings):
                    continue
                try:
                    parent_html = str(a.parent.parent.parent)
                except Exception:
                    parent_html = str(a.parent) if a.parent else ""
                snippet = BeautifulSoup(parent_html, "html.parser")
                container_text = snippet.get_text(separator=" | ", strip=True)
                parts = [p.strip() for p in container_text.split("|") if p.strip()]
                clean_parts = [p for p in parts if not re.match(r'^(view\s+listing|view\s+full|view\s+photo|#\d+)$', p, re.IGNORECASE)]
                name = ""
                for p in clean_parts:
                    if re.match(r'^(sat|sun|mon|tue|wed|thu|fri)\s+', p, re.IGNORECASE): continue
                    if re.match(r'^\d{1,2}/\d{1,2}', p): continue
                    if re.match(r'^by\s+', p, re.IGNORECASE): continue
                    if re.search(r',\s*[A-Z]{2}$', p): continue
                    if len(p) > 6:
                        name = p[:120]
                        break
                listing = {
                    "id": f"az_{lid}", "source": "auctionzip.com",
                    "url": f"https://www.auctionzip.com/listings/{lid}.html",
                    "name": name or f"Auction #{lid}",
                    "category": "auction", "state": state.upper(),
                    "city": "", "address": "", "zip": "",
                    "dates": "", "times": "", "company": "",
                    "distance": "", "photo_count": 0, "status": "",
                    "lat": None, "lng": None,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }
                for p in clean_parts:
                    dt_m = re.match(r'(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+)?((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)', p, re.IGNORECASE)
                    if dt_m:
                        listing["dates"] = dt_m.group(1).strip()
                        time_m = re.search(r'(\d{1,2}(?::\d{2})?\s*(?:AM|PM)(?:\s*[-–to]+\s*\d{1,2}(?::\d{2})?\s*(?:AM|PM))?)', p, re.IGNORECASE)
                        if time_m:
                            listing["times"] = time_m.group(1).strip()
                        break
                for p in clean_parts:
                    cs = re.match(r'^([A-Za-z][A-Za-z\s\.]{1,30}),\s*([A-Z]{2})$', p.strip())
                    if cs:
                        listing["city"] = cs.group(1).strip().title()
                        listing["state"] = cs.group(2)
                        break
                for p in clean_parts:
                    if re.match(r'^by\s+', p, re.IGNORECASE):
                        listing["company"] = re.sub(r'^by\s+', '', p, flags=re.IGNORECASE).strip()[:80]
                        listing["company"] = re.sub(r'\s*\(#\d+\)', '', listing["company"]).strip()
                        break
                listings.append(listing)
                found += 1
            print(f"     {state.upper()}: {found} listings")
            time.sleep(2)
        except Exception as e:
            print(f"     ❌ {state.upper()} error: {e}")
    return listings


# ══════════════════════════════════════════════════════════
# GEOCODING
# ══════════════════════════════════════════════════════════

def geocode_listings(listings, origin_lat, origin_lng, max_distance):
    print(f"  📍 Geocoding {len(listings)} listings...")
    geocoded = []
    for i, listing in enumerate(listings):
        addr  = listing.get("address", "")
        city  = listing.get("city", "")
        state = listing.get("state", "")
        zip_c = listing.get("zip", "")

        if re.search(r'\d+\s*(hours?|days?|weeks?)\s*ago|Pict', addr, re.IGNORECASE):
            listing["address"] = ""
            addr = ""

        queries = []
        if addr and city:
            queries.append(f"{addr}, {city}, {state} {zip_c}".strip(", "))
        if city:
            queries.append(f"{city}, {state} {zip_c}".strip())
        if zip_c:
            queries.append(zip_c)

        placed = False
        for query in queries:
            lat, lng = geocode(query)
            if lat:
                if origin_lat and origin_lng:
                    miles = haversine_miles(origin_lat, origin_lng, lat, lng)
                    if miles > max_distance:
                        break
                    listing["distance"] = miles
                listing["lat"] = lat
                listing["lng"]  = lng
                geocoded.append(listing)
                placed = True
                break
            time.sleep(1.0)

        if not placed:
            print(f"     ✗ skipped: {listing['name'][:45]}")

        if i < len(listings) - 1:
            time.sleep(1.2)

    print(f"     Geocoded: {len(geocoded)}/{len(listings)}")
    return geocoded


# ══════════════════════════════════════════════════════════
# GOOGLE PLACES SCRAPER
# ══════════════════════════════════════════════════════════

def scrape_google_places(origin_lat, origin_lng, radius_miles):
    listings = []
    seen_ids = set()
    radius_meters = min(int(radius_miles * 1609), 50000)
    print(f"  🗺️  Google Places: {radius_miles}mi radius...")

    for search_term, category in PLACES_SEARCHES:
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": f"{origin_lat},{origin_lng}",
            "radius": radius_meters,
            "keyword": search_term,
            "key": GOOGLE_API_KEY,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
            results = data.get("results", [])
            found = 0
            for place in results:
                place_id = place.get("place_id", "")
                if place_id in seen_ids:
                    continue
                seen_ids.add(place_id)
                loc = place.get("geometry", {}).get("location", {})
                lat = loc.get("lat")
                lng = loc.get("lng")
                if not lat or not lng:
                    continue
                miles = haversine_miles(origin_lat, origin_lng, lat, lng)
                if miles > radius_miles:
                    continue
                name = place.get("name", "")
                vicinity = place.get("vicinity", "")
                parts = [p.strip() for p in vicinity.split(",")]
                address = parts[0] if parts else ""
                city = parts[1].strip() if len(parts) > 1 else ""
                listing = {
                    "id":          f"gp_{place_id}",
                    "name":        name,
                    "category":    category,
                    "lat":         lat, "lng": lng,
                    "address":     address, "city": city,
                    "state":       "", "zip": "",
                    "dates":       "", "times": "",
                    "distance":    miles,
                    "rating":      place.get("rating"),
                    "tags":        [],
                    "description": "Listed by Google Places",
                    "status":      "",
                    "url":         f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                    "company":     "",
                    "source":      "google_places",
                    "photo_count": len(place.get("photos", [])),
                    "scraped_at":  datetime.now(timezone.utc).isoformat(),
                }
                listings.append(listing)
                found += 1

            # Pagination
            next_token = data.get("next_page_token")
            if next_token:
                time.sleep(2)
                resp2 = requests.get(url, params={"pagetoken": next_token, "key": GOOGLE_API_KEY}, timeout=15)
                for place in resp2.json().get("results", []):
                    place_id = place.get("place_id", "")
                    if place_id in seen_ids:
                        continue
                    seen_ids.add(place_id)
                    loc = place.get("geometry", {}).get("location", {})
                    lat, lng = loc.get("lat"), loc.get("lng")
                    if not lat or not lng:
                        continue
                    miles = haversine_miles(origin_lat, origin_lng, lat, lng)
                    if miles > radius_miles:
                        continue
                    vicinity = place.get("vicinity", "")
                    parts = [p.strip() for p in vicinity.split(",")]
                    listings.append({
                        "id": f"gp_{place_id}", "name": place.get("name",""),
                        "category": category, "lat": lat, "lng": lng,
                        "address": parts[0] if parts else "", "city": parts[1].strip() if len(parts)>1 else "",
                        "state": "", "zip": "", "dates": "", "times": "",
                        "distance": miles, "rating": place.get("rating"),
                        "tags": [], "description": "Listed by Google Places",
                        "status": "",
                        "url": f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                        "company": "", "source": "google_places",
                        "photo_count": len(place.get("photos",[])),
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                    })
                    found += 1

            print(f"     '{search_term}': {found} results")
            time.sleep(0.5)

        except Exception as e:
            print(f"     ❌ '{search_term}' error: {e}")

    print(f"     Google Places total: {len(listings)} unique")
    return listings


# ══════════════════════════════════════════════════════════
# SUPABASE PUSH
# ══════════════════════════════════════════════════════════

def push_to_supabase(listings):
    print(f"\n☁️  Pushing {len(listings)} listings to Supabase...")
    endpoint = f"{SUPABASE_URL}/rest/v1/listings"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
        "X-Upsert": "true",
    }

    rows = []
    for l in listings:
        dist = l.get("distance", None)
        if isinstance(dist, str):
            m = re.search(r'[\d.]+', dist)
            dist = float(m.group()) if m else None
        rows.append({
            "id":          l["id"],
            "name":        l.get("name", "Unknown"),
            "category":    l.get("category", "estate"),
            "lat":         l.get("lat"),
            "lng":         l.get("lng"),
            "address":     l.get("address", ""),
            "city":        l.get("city", ""),
            "state":       l.get("state", ""),
            "dates":       l.get("dates", ""),
            "times":       l.get("times", ""),
            "distance":    dist,
            "rating":      l.get("rating"),
            "tags":        l.get("tags", []),
            "description": l.get("description", ""),
            "status":      l.get("status", ""),
            "url":         l.get("url", ""),
            "company":     l.get("company", ""),
            "source":      l.get("source", "estatesales.net"),
            "photo_count": l.get("photo_count", 0),
            "scraped_at":  l.get("scraped_at", datetime.now(timezone.utc).isoformat()),
        })

    batch_size = 50
    success = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        try:
            resp = requests.post(endpoint, json=batch, headers=headers, timeout=30)
            if resp.status_code in (200, 201):
                success += len(batch)
            else:
                print(f"   ❌ Batch {i//batch_size + 1} failed: {resp.status_code} — {resp.text[:200]}")
        except Exception as e:
            print(f"   ❌ Batch error: {e}")

    print(f"   ✅ Pushed {success}/{len(listings)}")
    return success


# ══════════════════════════════════════════════════════════
# CLEANUP — remove expired estate sale / auction listings
# NOTE: Runs AFTER push so fresh dates are in DB before expiry check
# ══════════════════════════════════════════════════════════

MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12
}

def parse_last_date(dates_str):
    """Extract the latest date mentioned in a dates string. Returns (month, day) or None."""
    if not dates_str:
        return None
    matches = re.findall(
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{1,2})',
        dates_str, re.IGNORECASE
    )
    if not matches:
        return None
    month_str, day_str = matches[-1]
    return MONTH_MAP.get(month_str.lower()[:3]), int(day_str)


def cleanup_expired_listings():
    """
    Delete expired listings:
    1. estatesales.net listings not re-scraped in 8+ days (site stops showing ended sales)
    2. Any listing whose dates have passed
    3. Listings with no dates scraped 7+ days ago
    """
    print("\n🧹 Cleaning up expired listings...")
    today = datetime.now(timezone.utc)
    current_month = today.month
    current_day   = today.day

    endpoint = f"{SUPABASE_URL}/rest/v1/listings"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.get(
            endpoint,
            params={
                "select": "id,dates,source,category,scraped_at",
                "source": "neq.google_places",
                "limit": 2000,
            },
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"   ⚠️  Couldn't fetch listings: {resp.status_code}")
            return
        listings = resp.json()
    except Exception as e:
        print(f"   ❌ Fetch error: {e}")
        return

    expired_ids = []
    stale_es = 0
    date_expired = 0
    age_expired = 0

    for l in listings:
        if l.get("source") == "google_places":
            continue

        scraped_at = l.get("scraped_at", "")

        # Rule 1: estatesales.net listings not seen in 8+ days
        # If EstateSales.NET still had this sale active, it would have been
        # re-scraped and upserted today, resetting scraped_at.
        # Anything older than 8 days was not on their site — sale is over.
        if l.get("source") == "estatesales.net" and scraped_at:
            try:
                scraped = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
                if (today - scraped).days >= 8:
                    expired_ids.append(l["id"])
                    stale_es += 1
                    continue
            except Exception:
                pass

        # Rule 2: dates field populated — check if end date has passed
        parsed = parse_last_date(l.get("dates", ""))
        if parsed:
            end_month, end_day = parsed
            if end_month > current_month + 1:
                expired_ids.append(l["id"])
                date_expired += 1
            elif end_month < current_month:
                expired_ids.append(l["id"])
                date_expired += 1
            elif end_month == current_month and end_day < current_day:
                expired_ids.append(l["id"])
                date_expired += 1
            continue

        # Rule 3: no dates, not estatesales.net — expire after 7 days
        if scraped_at:
            try:
                scraped = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
                if (today - scraped).days > 7:
                    expired_ids.append(l["id"])
                    age_expired += 1
            except Exception:
                pass

    if not expired_ids:
        print("   ✅ No expired listings found")
        return

    print(f"   Found {len(expired_ids)} expired: {stale_es} stale ES, {date_expired} date-expired, {age_expired} age-expired — deleting...")

    deleted = 0
    batch_size = 50
    for i in range(0, len(expired_ids), batch_size):
        batch = expired_ids[i:i+batch_size]
        id_filter = "(" + ",".join(f'"{id}"' for id in batch) + ")"
        try:
            del_resp = requests.delete(
                endpoint,
                params={"id": f"in.{id_filter}"},
                headers=headers,
                timeout=30,
            )
            if del_resp.status_code in (200, 204):
                deleted += len(batch)
            else:
                print(f"   ❌ Delete batch failed: {del_resp.status_code}")
        except Exception as e:
            print(f"   ❌ Delete error: {e}")

    print(f"   ✅ Deleted {deleted} expired listings")
    
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"   ⚠️  Couldn't fetch listings: {resp.status_code}")
            return
        listings = resp.json()
    except Exception as e:
        print(f"   ❌ Fetch error: {e}")
        return

    expired_ids = []
    no_date_expired = 0

    for l in listings:
        if l.get("source") == "google_places":
            continue

        parsed = parse_last_date(l.get("dates", ""))

        if not parsed:
            # Fallback: no date stored — expire if scraped more than 7 days ago
            scraped_at = l.get("scraped_at", "")
            if scraped_at:
                try:
                    scraped = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
                    if (today - scraped).days > 7:
                        expired_ids.append(l["id"])
                        no_date_expired += 1
                except Exception:
                    pass
            continue

        end_month, end_day = parsed

        # Handle year-wrap: if end month is way ahead of current, it's likely last year
        # e.g. cleanup runs in Jan and finds a "Dec 28" listing → expired
        if end_month > current_month + 1:
            # Month is more than 1 ahead — treat as prior year, already expired
            expired_ids.append(l["id"])
        elif end_month < current_month:
            expired_ids.append(l["id"])
        elif end_month == current_month and end_day < current_day:
            expired_ids.append(l["id"])

    if not expired_ids:
        print("   ✅ No expired listings found")
        return

    print(f"   Found {len(expired_ids)} expired listings ({no_date_expired} by age fallback) — deleting...")

    deleted = 0
    batch_size = 50
    for i in range(0, len(expired_ids), batch_size):
        batch = expired_ids[i:i+batch_size]
        id_filter = "(" + ",".join(f'"{id}"' for id in batch) + ")"
        try:
            del_resp = requests.delete(
                endpoint,
                params={"id": f"in.{id_filter}"},
                headers=headers,
                timeout=30,
            )
            if del_resp.status_code in (200, 204):
                deleted += len(batch)
            else:
                print(f"   ❌ Delete batch failed: {del_resp.status_code}")
        except Exception as e:
            print(f"   ❌ Delete error: {e}")

    print(f"   ✅ Deleted {deleted} expired listings")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main():
    print("=" * 55)
    print(f"   🔍 Fade & Find — Scraper v2 [{MODE}]")
    print("=" * 55)

    if not SUPABASE_KEY:
        print("❌ SUPABASE_SERVICE_KEY not set")
        sys.exit(1)

    metros = DAILY_METROS if MODE == "daily" else US_METROS
    all_listings = []
    seen_ids = set()

    for state, city, zip_code, radius in metros:
        print(f"\n📍 {city}, {state} ({zip_code}) — {radius}mi")

        origin_lat, origin_lng = geocode(f"{city.replace('-',' ')}, {state} {zip_code}")
        if not origin_lat:
            print(f"   ⚠️  Couldn't geocode {city}, {state} — skipping")
            continue
        time.sleep(1.5)

        city_url = city.replace(" ", "-")

        # Estate sales
        es = scrape_estatesales(state, city_url, zip_code)
        es = geocode_listings(es, origin_lat, origin_lng, radius)
        for l in es:
            if l["id"] not in seen_ids:
                seen_ids.add(l["id"])
                all_listings.append(l)

        # AuctionZip
        az = scrape_auctionzip([state.lower()], origin_lat, origin_lng, radius)
        az = geocode_listings(az, origin_lat, origin_lng, radius)
        for l in az:
            if l["id"] not in seen_ids:
                seen_ids.add(l["id"])
                all_listings.append(l)

        # Google Places — weekly/full only
        if MODE in ("weekly", "full") and GOOGLE_API_KEY:
            gp = scrape_google_places(origin_lat, origin_lng, radius)
            for l in gp:
                if l["id"] not in seen_ids:
                    seen_ids.add(l["id"])
                    all_listings.append(l)

        print(f"   Running total: {len(all_listings)} listings")
        time.sleep(2)

    # Push first, then clean — cleanup uses freshly upserted dates
    if all_listings:
        push_to_supabase(all_listings)
    else:
        print("\n⚠️  No listings to push.")

    cleanup_expired_listings()

    print(f"\n{'='*55}")
    print(f"   ✅ Done. Total pushed: {len(all_listings)}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
