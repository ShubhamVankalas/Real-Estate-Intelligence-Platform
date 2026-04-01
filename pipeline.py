import pandas as pd
import requests
from bs4 import BeautifulSoup
import feedparser
import json
import time
import re
import uuid
from ai_utils import extract_entities, generate_insights

import os
from dotenv import load_dotenv

load_dotenv()

# Config
DATA_DIR = "data"
FMP_API_KEY = os.getenv("FMP_API_KEY")
GBP_TO_USD = 1.27
EUR_TO_USD = 1.08

RSS_FEEDS = {
    "Property Week Finance": "https://www.propertyweek.com/finance/feed",
    "Property Week News":    "https://www.propertyweek.com/news/feed",
}
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Record template
def blank_record():
    return {
        "record_id":      str(uuid.uuid4())[:8],
        "source":         None,
        "source_type":    None,   # structured | rss | web_scrape | api
        "city_key":       None,   # normalised city name (lowercase)
        "city_display":   None,
        "country_region": None,
        "asset_class":    None,
        "date":           None,
        "loan_amount_usd": None,
        "ltv_ratio":      None,
        "list_price_usd": None,
        "price_per_sqft": None,
        "lender":         None,
        "borrower":       None,
        "lat":            None,
        "lon":            None,
        "year_built":     None,
        "ai_sentiment":   None,
        "ai_summary":     None,
        "ai_topics":      [],
        "ai_entities":    [],
        "fmp_ticker":     None,
        "fmp_stock_price":None,
        "fmp_market_cap": None,
        # raw extras
        "title":          None,
        "url":            None,
        "content":        None,
        "notes":          None,
    }


# Step 1: cities.csv -> DMS decimal lat/lon lookup
def build_city_key_map() -> dict:
    """Parse DMS coordinates from cities.csv → decimal lat/lon keyed by city name."""
    df = pd.read_csv(f"{DATA_DIR}\\cities.csv", quotechar='"', skipinitialspace=True)
    df.columns = [c.strip().strip('"') for c in df.columns]
    city_map = {}
    for _, row in df.iterrows():
        city = str(row.get("City", "")).strip().strip('"')
        state = str(row.get("State", "")).strip().strip('"')
        if not city or city == "nan":
            continue
        try:
            lat_d, lat_m, lat_s = float(row["LatD"]), float(row["LatM"]), float(row["LatS"])
            lon_d, lon_m, lon_s = float(row["LonD"]), float(row["LonM"]), float(row["LonS"])
            lat = lat_d + lat_m / 60 + lat_s / 3600
            lon = -(lon_d + lon_m / 60 + lon_s / 3600)  # all W hemisphere
            city_map[city.lower()] = {
                "city": city,
                "state": state,
                "lat": round(lat, 4),
                "lon": round(lon, 4),
            }
        except Exception:
            pass
    print(f"City lookup built: {len(city_map)} cities")
    return city_map


# Step 2: homes.csv -> US residential price benchmark
def ingest_homes(city_map: dict) -> list:
    """homes.csv has no city column — treat as generic US residential benchmark."""
    df = pd.read_csv(f"{DATA_DIR}\\homes.csv", quotechar='"', skipinitialspace=True)
    df.columns = [c.strip().strip('"') for c in df.columns]
    records = []
    for _, row in df.iterrows():
        sell = _to_float(row.get("Sell"))
        living = _to_float(row.get("Living"))
        price_usd = sell * 1000 if sell else None          # values are in $000s
        ppsf = round(price_usd / living, 2) if price_usd and living else None
        r = blank_record()
        r.update({
            "source":         "Homes Dataset",
            "source_type":    "csv",
            "city_key":       "generic_us",
            "city_display":   "US (Benchmark)",
            "country_region": "US",
            "asset_class":    "Residential",
            "list_price_usd": price_usd,
            "price_per_sqft": ppsf,
            "year_built":     int(row["Age"]) if _to_float(row.get("Age")) else None,
            "notes":          f"Beds:{row.get('Beds')} Baths:{row.get('Baths')} Acres:{row.get('Acres')} Taxes:{row.get('Taxes')}",
        })
        records.append(r)
    print(f"Homes: {len(records)} records")
    return records


# Step 3: zillow.csv -> Tallahassee FL listings
def ingest_zillow(city_map: dict) -> list:
    """zillow zips are all 323xx = Tallahassee, FL — look up lat/lon from city_map."""
    df = pd.read_csv(f"{DATA_DIR}\\zillow.csv", quotechar='"', skipinitialspace=True)
    df.columns = [c.strip().strip('"') for c in df.columns]
    tallahassee = city_map.get("tallahassee", {})
    records = []
    for _, row in df.iterrows():
        sqft = _to_float(row.get("Living Space (sq ft)"))
        price = _to_float(row.get("List Price ($)"))
        ppsf = round(price / sqft, 2) if price and sqft else None
        r = blank_record()
        r.update({
            "source":         "Zillow Dataset",
            "source_type":    "csv",
            "city_key":       "tallahassee",
            "city_display":   "Tallahassee, FL",
            "country_region": "US",
            "asset_class":    "Residential",
            "list_price_usd": price,
            "price_per_sqft": ppsf,
            "year_built":     int(row["Year"]) if _to_float(row.get("Year")) else None,
            "lat":            tallahassee.get("lat"),
            "lon":            tallahassee.get("lon"),
            "notes":          f"Zip:{row.get('Zip')} Beds:{row.get('Beds')} Baths:{row.get('Baths')}",
        })
        records.append(r)
    print(f"Zillow: {len(records)} records")
    return records


# Step 4: CRE Lending .xlsx -> parse city, LTV, convert currency
def ingest_cre_excel(city_map: dict) -> list:
    filepath = f"{DATA_DIR}\\Real-Estate-Capital-Europe-Sample-CRE-Lending-Data.xlsx"
    xls = pd.ExcelFile(filepath)
    records = []

    for sheet in xls.sheet_names:
        is_uk = "UK" in sheet
        currency = "GBP" if is_uk else "EUR"
        fx = GBP_TO_USD if is_uk else EUR_TO_USD
        region = "UK" if is_uk else "Continental Europe"

        df_raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        header_row = None
        for i, row in df_raw.iterrows():
            if "Lender" in [str(v).strip() for v in row.values]:
                header_row = i
                break
        if header_row is None:
            continue

        df = pd.read_excel(xls, sheet_name=sheet, header=header_row).dropna(how="all")
        loan_col = next((c for c in df.columns if "loan size" in c.lower()), None)

        current_date = None
        for _, row in df.iterrows():
            lender = str(row.get("Lender", "")).strip()
            if re.match(r"\d{4}-\d{2}-\d{2}", lender):
                current_date = lender[:7]
                continue
            if not lender or lender.lower() == "nan":
                continue

            # Extract city from Asset(s) column
            asset_str = str(row.get("Asset(s)", "")).strip()
            if asset_str.lower() == "nan":
                asset_str = ""
            city_key, city_display, lat, lon = _extract_city_from_asset(asset_str, city_map)

            # Classify asset class
            asset_class = _classify_asset(asset_str)

            # Parse loan size → USD
            loan_m = None
            loan_usd = None
            if loan_col:
                raw = row.get(loan_col, "")
                if pd.notna(raw):
                    loan_m = _to_float(str(raw))
                    if loan_m:
                        loan_usd = round(loan_m * 1_000_000 * fx, 0)

            # Parse LTV from Notes
            notes = str(row.get("Notes", "")).strip()
            if notes.lower() == "nan":
                notes = ""
            ltv = _parse_ltv(notes)

            borrower = str(row.get("Borrower", "")).strip()
            if borrower.lower() == "nan":
                borrower = ""

            r = blank_record()
            r.update({
                "source":         "CRE Lending Data",
                "source_type":    "structured",
                "city_key":       city_key,
                "city_display":   city_display,
                "country_region": region,
                "asset_class":    asset_class,
                "date":           current_date,
                "loan_amount_usd": loan_usd,
                "ltv_ratio":      ltv,
                "lender":         lender,
                "borrower":       borrower,
                "lat":            lat,
                "lon":            lon,
                "notes":          notes[:500],
            })
            records.append(r)

    print(f"CRE Lending: {len(records)} deals parsed")
    return records


# Step 5: RSS Feeds -> Property Week
def ingest_rss() -> list:
    records = []
    for feed_name, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                content = getattr(entry, "summary", getattr(entry, "description", ""))
                clean = BeautifulSoup(content, "html.parser").get_text()
                pub = entry.get("published", "")
                date_str = pub[:7] if len(pub) >= 7 else pub
                r = blank_record()
                r.update({
                    "source":      feed_name,
                    "source_type": "rss",
                    "title":       entry.get("title", ""),
                    "url":         entry.get("link", ""),
                    "date":        date_str,
                    "content":     clean[:2000],
                })
                records.append(r)
        except Exception as e:
            print(f"  ⚠ RSS error {feed_name}: {e}")
    print(f"RSS: {len(records)} articles")
    return records


# Step 6: Web Scraping - JLL + Altus
def scrape_articles(url: str, source_name: str, limit: int = 6) -> list:
    records = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.content, "html.parser")
        seen = set()
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(kw in href for kw in ["/insights/", "/trends/", "/research/", "/article/", "/news/"]):
                if href.startswith("/"):
                    href = "/".join(url.split("/")[:3]) + href
                if href not in seen and len(href) > 30:
                    seen.add(href)
                    links.append(href)
        for link in links[:limit]:
            try:
                res = requests.get(link, headers=HEADERS, timeout=15)
                s = BeautifulSoup(res.content, "html.parser")
                title_tag = s.find("h1")
                title = title_tag.get_text().strip() if title_tag else link.split("/")[-1].replace("-", " ").title()
                paras = s.find_all("p")
                text = " ".join(p.get_text().strip() for p in paras[:8])
                if len(text) > 80:
                    r = blank_record()
                    r.update({
                        "source":      source_name,
                        "source_type": "web_scrape",
                        "title":       title,
                        "url":         link,
                        "content":     text[:2500],
                    })
                    records.append(r)
                time.sleep(1.5)
            except Exception:
                pass
    except Exception as e:
        print(f"  ⚠ Scrape failed {url}: {e}")
    return records


def ingest_web() -> list:
    print("  Scraping JLL...")
    jll = scrape_articles("https://www.jll.co.uk/en/trends-and-insights", "JLL Insights", limit=6)
    print(f"JLL: {len(jll)} articles")
    print("  Scraping Altus...")
    altus = scrape_articles("https://www.altusgroup.com/insights/", "Altus Group", limit=6)
    print(f"Altus: {len(altus)} articles")
    return jll + altus


# Step 7: AI Enrichment on articles
def enrich_articles_with_ai(article_records: list, city_map: dict) -> list:
    """Run entity extraction on articles, fill in city_key, asset_class, sentiment."""
    extracted_companies = []
    for i, r in enumerate(article_records):
        text = f"Title: {r.get('title','')}\n{r.get('content','')}"
        print(f"  Article {i+1}/{len(article_records)} [{r['source']}]")
        ents = extract_entities(text, source_type="news")
        if isinstance(ents, dict):
            # Fill city_key from first location AI found
            locs = ents.get("locations", [])
            if locs:
                ck = locs[0].lower().split(",")[0].strip()
                found = city_map.get(ck)
                if found:
                    r["city_key"] = ck
                    r["city_display"] = found["city"]
                    r["lat"] = found["lat"]
                    r["lon"] = found["lon"]
                else:
                    r["city_key"] = ck
                    r["city_display"] = locs[0]
            # Asset class
            at = ents.get("asset_types", [])
            if at:
                r["asset_class"] = at[0]
            # Sentiment
            r["ai_sentiment"] = ents.get("sentiment")
            r["ai_summary"]   = ents.get("summary")
            r["ai_topics"]    = ents.get("asset_types", [])
            r["ai_entities"]  = ents.get("companies", [])
            extracted_companies.extend(ents.get("companies", []))
    return article_records, list(set(extracted_companies))


# Step 8: FMP API query by AI-extracted companies
def ingest_fmp_by_companies(company_names: list) -> dict:
    """Query FMP by real company names extracted from articles (JLL, CBRE, Aviva, etc.)"""
    profiles = {}
    # Prioritise well-known real estate companies likely to be on FMP
    priority = ["JLL", "CBRE", "Aviva", "British Land", "Segro", "Landsec", "Derwent", 
                "Hammerson", "Tritax", "Cushman", "Savills", "Brookfield", "Blackstone", "Prologis"]
    # Merge with AI-extracted names, dedup
    all_names = priority + [n for n in company_names if n not in priority]
    queried = 0
    for name in all_names[:20]:
        if queried >= 15:
            break
        try:
            url = f"https://financialmodelingprep.com/api/v3/search?query={requests.utils.quote(name)}&limit=1&apikey={FMP_API_KEY}"
            res = requests.get(url, timeout=10).json()
            if not res or not isinstance(res, list):
                continue
            ticker = res[0].get("symbol")
            exchange = res[0].get("exchangeShortName", "")
            # Only take stocks on major exchanges (avoid obscure matches)
            if not ticker or exchange not in ["NYSE", "NASDAQ", "LSE", "XLON", "NasdaqGS", "NasdaqGM"]:
                # Try anyway if name matches
                if not ticker:
                    continue
            profile_url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
            prof = requests.get(profile_url, timeout=10).json()
            if prof and isinstance(prof, list) and len(prof) > 0:
                p = prof[0]
                profiles[name] = {
                    "ticker":      ticker,
                    "exchange":    exchange,
                    "sector":      p.get("sector"),
                    "industry":    p.get("industry"),
                    "mkt_cap":     p.get("mktCap"),
                    "price":       p.get("price"),
                    "country":     p.get("country"),
                    "description": (p.get("description") or "")[:200],
                }
                queried += 1
                print(f"FMP: {name} -> {ticker}")
            time.sleep(0.5)
        except Exception:
            pass
    print(f"FMP: matched {len(profiles)} company profiles")
    return profiles



# Step 9: FMP profiles into article records
def link_fmp_to_articles(article_records: list, fmp_profiles: dict) -> list:
    for r in article_records:
        companies = r.get("ai_entities", [])
        for company in companies:
            if company in fmp_profiles:
                p = fmp_profiles[company]
                r["fmp_ticker"]      = p["ticker"]
                r["fmp_stock_price"] = p["price"]
                r["fmp_market_cap"]  = p["mkt_cap"]
                break  # use first match
    return article_records


# Step 10: AI Enrich CRE deals (notes only)
def enrich_deals_with_ai(deal_records: list) -> list:
    """Run entity extraction on deal notes to get finer asset_class and LTV."""
    enriched = 0
    for i, r in enumerate(deal_records[:25]):
        notes = r.get("notes", "") or ""
        if len(notes) < 30:
            continue
        text = f"Lender: {r.get('lender','')}\nBorrower: {r.get('borrower','')}\nNotes: {notes}"
        ents = extract_entities(text, source_type="lending")
        if isinstance(ents, dict):
            figs = ents.get("key_figures", {}) or {}
            # Override LTV if AI found it and we didn't regex-parse it
            if r["ltv_ratio"] is None and figs.get("ltv_pct"):
                r["ltv_ratio"] = _to_float(str(figs["ltv_pct"]))
            # Refine asset_class if AI is more specific
            at = ents.get("asset_types", [])
            if at and r["asset_class"] in ("Mixed-Use/Other", None):
                r["asset_class"] = at[0]
            r["ai_sentiment"] = ents.get("sentiment")
            r["ai_summary"]   = ents.get("summary")
            r["ai_topics"]    = at
            enriched += 1
        print(f"  Deal {i+1} [{r.get('lender','')[:25]}]")
    print(f"AI enriched {enriched} deals")
    return deal_records


# Helper functions
def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _parse_ltv(notes: str) -> float | None:
    """Extract LTV % from notes text."""
    if not notes:
        return None
    match = re.search(r"(\d+\.?\d*)\s*%?\s*(?:LTV|loan.to.value)", notes, re.IGNORECASE)
    return _to_float(match.group(1)) if match else None


# UK/EU cities commonly found in CRE asset descriptions
UK_EU_CITIES = {
    "london": (51.5074, -0.1278), "manchester": (53.4808, -2.2426),
    "birmingham": (52.4862, -1.8904), "edinburgh": (55.9533, -3.1883),
    "glasgow": (55.8642, -4.2518), "bristol": (51.4545, -2.5879),
    "leeds": (53.8008, -1.5491), "sheffield": (53.3811, -1.4701),
    "liverpool": (53.4084, -2.9916), "oxford": (51.752, -1.2577),
    "cambridge": (52.2053, 0.1218), "brighton": (50.8225, -0.1372),
    "cardiff": (51.4816, -3.1791), "dublin": (53.3498, -6.2603),
    "paris": (48.8566, 2.3522), "amsterdam": (52.3676, 4.9041),
    "berlin": (52.52, 13.405), "frankfurt": (50.1109, 8.6821),
    "munich": (48.1351, 11.582), "hamburg": (53.5753, 10.0153),
    "madrid": (40.4168, -3.7038), "barcelona": (41.3851, 2.1734),
    "milan": (45.4654, 9.1859), "rome": (41.9028, 12.4964),
    "warsaw": (52.2297, 21.0122), "prague": (50.0755, 14.4378),
    "brussels": (50.8503, 4.3517), "vienna": (48.2082, 16.3738),
    "zurich": (47.3769, 8.5417), "stockholm": (59.3293, 18.0686),
    "copenhagen": (55.6761, 12.5683), "oslo": (59.9139, 10.7522),
    "helsinki": (60.1699, 24.9384), "lisbon": (38.7223, -9.1393),
    "belgravia": (51.4994, -0.1548), "canary wharf": (51.5054, -0.0235),
    "southwark": (51.5039, -0.0872), "mayfair": (51.5118, -0.1445),
    "hannover": (52.3759, 9.732), "dusseldorf": (51.2217, 6.7762),
    "cologne": (50.938, 6.9603), "rotterdam": (51.9244, 4.4777),
    "surrey": (51.3148, -0.559), "yorkshire": (53.9591, -1.0815),
    "edinburgh": (55.9533, -3.1883),
}


def _extract_city_from_asset(asset_str: str, city_map: dict):
    """Extract city name from Asset description, return (city_key, city_display, lat, lon)."""
    if not asset_str:
        return None, None, None, None
    # Try to find known EU/UK cities first
    asset_lower = asset_str.lower()
    for city_name, (lat, lon) in UK_EU_CITIES.items():
        if city_name in asset_lower:
            return city_name, city_name.title(), lat, lon
    # Try US city_map too
    for city_key, info in city_map.items():
        if city_key in asset_lower:
            return city_key, info["city"], info["lat"], info["lon"]
    # Last resort: grab text after comma
    parts = asset_str.split(",")
    if len(parts) > 1:
        guess = parts[-1].strip().lower()
        return guess, parts[-1].strip(), None, None
    return None, None, None, None


def _classify_asset(asset_str: str) -> str:
    s = asset_str.lower()
    if any(kw in s for kw in ["office", "offices"]):                        return "Office"
    if any(kw in s for kw in ["residential", "apartment", "flat", "homes"]): return "Residential"
    if any(kw in s for kw in ["hotel", "hospitality"]):                     return "Hotel"
    if any(kw in s for kw in ["retail", "shopping", "mall"]):               return "Retail"
    if any(kw in s for kw in ["industrial", "logistics", "warehouse"]):     return "Industrial"
    if any(kw in s for kw in ["student", "dormitory"]):                     return "Student Housing"
    if any(kw in s for kw in ["mixed", "multi"]):                           return "Mixed-Use"
    return "Mixed-Use/Other"


# Main pipeline execution
def main():
    print("Starting pipeline...")
    print("\n[1/8] Building city key map from cities.csv...")
    city_map = build_city_key_map()

    # Step 2: Ingest CSVs → unified records
    print("\n[2/8] Ingesting homes.csv (US residential benchmark)...")
    home_records = ingest_homes(city_map)

    print("\n[3/8] Ingesting zillow.csv (Tallahassee, FL listings)...")
    zillow_records = ingest_zillow(city_map)

    # Step 3: Ingest CRE Excel
    print("\n[4/8] Ingesting CRE Lending Excel (UK + Continental Europe)...")
    deal_records = ingest_cre_excel(city_map)

    # Step 4: RSS
    print("\n[5/8] Ingesting Property Week RSS feeds...")
    rss_records = ingest_rss()

    # Step 5: Web scraping
    print("\n[6/8] Scraping JLL + Altus Group articles...")
    web_records = ingest_web()
    article_records = rss_records + web_records

    # Step 6: AI enrich articles → extract companies for FMP
    print(f"\n[7/8] AI enrichment ({len(article_records)} articles)...")
    article_records, extracted_companies = enrich_articles_with_ai(article_records, city_map)
    print(f"  Companies extracted by AI: {extracted_companies[:10]}")

    # AI enrich deals (notes)
    print(f"\n  AI enriching deals ({min(25, len(deal_records))} deals)...")
    deal_records = enrich_deals_with_ai(deal_records)

    # Step 7: FMP API
    print("\n[8/8] FMP API — querying company profiles...")
    fmp_profiles = ingest_fmp_by_companies(extracted_companies)

    # Link FMP back to articles
    article_records = link_fmp_to_articles(article_records, fmp_profiles)

    # Merge ALL into single unified records array
    all_records = home_records + zillow_records + deal_records + article_records

    # Generate cross-source insights
    print("\nGenerating cross-source insights...")
    insights = generate_insights(all_records, fmp_profiles)

    # Final output
    output = {
        "records":     all_records,
        "fmp_profiles": fmp_profiles,
        "city_map":    city_map,   # kept for reference / map rendering
        "insights":    insights,
        "meta": {
            "total_records":   len(all_records),
            "deal_records":    len(deal_records),
            "article_records": len(article_records),
            "home_records":    len(home_records) + len(zillow_records),
            "fmp_matched":     len(fmp_profiles),
            "insights_count":  len(insights),
        }
    }

    with open("unified_dataset.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str, ensure_ascii=False)

    print("\nPipeline complete.")
    print(f"Total records:   {len(all_records)}")
    print(f"CRE deals:       {len(deal_records)}")
    print(f"Article records: {len(article_records)}")
    print(f"Housing records: {len(home_records)+len(zillow_records)}")
    print(f"FMP profiles:    {len(fmp_profiles)}")
    print(f"Insights:        {len(insights)}")
    print("Output saved to unified_dataset.json")


if __name__ == "__main__":
    main()
