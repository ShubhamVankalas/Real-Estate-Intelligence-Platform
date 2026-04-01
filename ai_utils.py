import os
import json
import time
import re
from groq import Groq

GROQ_API_KEY = st.secrets("GROQ_API_KEY")
MODEL = st.secrets("GROQ_MODEL")
RATE_LIMIT_DELAY = 3

client = Groq(api_key=GROQ_API_KEY)


def call_llm(system_prompt: str, user_prompt: str, temperature: float = 0.2, max_tokens: int = 1024) -> str | None:
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=temperature,
                max_tokens=max_tokens,
            )
            time.sleep(RATE_LIMIT_DELAY)
            return response.choices[0].message.content
        except Exception as e:
            err = str(e).lower()
            if "rate_limit" in err or "429" in err:
                wait = 12 * (attempt + 1)
                print(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"LLM error: {e}")
                return None
    return None


def _parse_json(text: str):
    if not text:
        return None
    text = text.strip()
    for prefix in ["```json", "```"]:
        if text.startswith(prefix):
            text = text[len(prefix):]
    if text.endswith("```"):
        text = text[:-3]
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        for s_char, e_char in [('{', '}'), ('[', ']')]:
            s = text.find(s_char)
            e = text.rfind(e_char)
            if s != -1 and e != -1 and e > s:
                try:
                    return json.loads(text[s:e+1])
                except json.JSONDecodeError:
                    continue
    return None


def extract_entities(text: str, source_type: str = "news") -> dict:
    system_prompt = """You are a Commercial Real Estate data extraction AI.
Extract structured information from the text. Return ONLY valid JSON.
{
  "locations": ["London", "Manchester"],
  "asset_types": ["Office", "Residential", "Retail", "Industrial", "Hotel", "Mixed-Use", "Student Housing", "Logistics"],
  "companies": ["JLL", "CBRE"],
  "sentiment": "Positive|Neutral|Negative",
  "key_figures": {"loan_size_m": null, "ltv_pct": null, "rate_pct": null},
  "summary": "One sentence summary."
}
Return empty arrays/null where not available."""
    result = call_llm(system_prompt, text[:2500], temperature=0.1)
    parsed = _parse_json(result)
    return parsed if isinstance(parsed, dict) else {}


def generate_insights(records: list, fmp_profiles: dict = None) -> list:
    """Generate insights from the unified record dataset."""
    fmp_profiles = fmp_profiles or {}

    # Compute real correlations before sending to LLM
    deal_recs   = [r for r in records if r.get("source_type") == "structured" and r.get("lender")]
    article_recs = [r for r in records if r.get("source_type") in ("rss", "web_scrape")]
    housing_recs = [r for r in records if r.get("source_type") == "csv"]

    # Sentiment by city
    city_sentiment: dict[str, list] = {}
    for a in article_recs:
        ck = a.get("city_key")
        sent = a.get("ai_sentiment")
        if ck and sent:
            city_sentiment.setdefault(ck, []).append(sent)
    city_avg_sent = {
        city: (sents.count("Positive") - sents.count("Negative")) / len(sents)
        for city, sents in city_sentiment.items() if sents
    }

    # LTV by city from deals
    city_ltv: dict[str, list] = {}
    for d in deal_recs:
        ck = d.get("city_key")
        ltv = d.get("ltv_ratio")
        if ck and ltv:
            city_ltv.setdefault(ck, []).append(ltv)
    city_avg_ltv = {city: round(sum(v)/len(v), 1) for city, v in city_ltv.items()}

    # Deal volume by asset class
    asset_deal_vol: dict[str, float] = {}
    for d in deal_recs:
        ac = d.get("asset_class", "Unknown") or "Unknown"
        usd = d.get("loan_amount_usd") or 0
        asset_deal_vol[ac] = asset_deal_vol.get(ac, 0) + usd

    # Article mentions by asset class
    asset_media: dict[str, int] = {}
    for a in article_recs:
        for t in (a.get("ai_topics") or []):
            asset_media[t] = asset_media.get(t, 0) + 1

    # Cities in BOTH news and deals
    news_cities  = set(a.get("city_key") for a in article_recs if a.get("city_key"))
    deal_cities  = set(d.get("city_key") for d in deal_recs  if d.get("city_key"))
    overlap      = news_cities & deal_cities
    blind_spots  = deal_cities - news_cities   # active lending but no news coverage
    media_only   = news_cities - deal_cities   # media hype with no deals

    # Housing price stats
    us_prices = [r.get("list_price_usd") for r in housing_recs if r.get("list_price_usd")]
    avg_us_price = round(sum(us_prices) / len(us_prices), 0) if us_prices else 0

    deal_usd = [d.get("loan_amount_usd") for d in deal_recs if d.get("loan_amount_usd")]
    avg_cre_deal = round(sum(deal_usd) / len(deal_usd), 0) if deal_usd else 0

    fmp_summary = []
    for cname, p in list(fmp_profiles.items())[:5]:
        fmp_summary.append(f"{cname} ({p.get('ticker')}) mktCap=${p.get('mkt_cap', 0)/1e9:.1f}B, sector={p.get('sector')}")

    context = f"""UNIFIED DATASET CROSS-SOURCE SUMMARY:

DEALS ({len(deal_recs)} CRE loans, UK + Continental Europe, 2018):
- Asset class deal volumes (USD): {dict(sorted(asset_deal_vol.items(), key=lambda x:-x[1])[:8])}
- Cities with avg LTV: {city_avg_ltv}
- Total average CRE loan: ${avg_cre_deal:,.0f}

ARTICLES ({len(article_recs)} from Property Week RSS, JLL, Altus Group):
- News sentiment by city (score +1=Positive, -1=Negative): {city_avg_sent}
- Media mentions by asset class: {asset_media}

GEOGRAPHIC CROSS-MATCH:
- Cities in BOTH news AND deals: {list(overlap)[:10]}
- Deal activity with NO news coverage (blind spots): {list(blind_spots)[:8]}
- Media hype with NO actual deals: {list(media_only)[:8]}

HOUSING MARKET (US benchmark: homes.csv, zillow Tallahassee):
- Average US residential listing price: ${avg_us_price:,.0f}
- Ratio: average CRE loan = {f"{avg_cre_deal/avg_us_price:.0f}x" if avg_us_price else "N/A"} a US home

PUBLIC MARKET (FMP API):
{chr(10).join(fmp_summary) if fmp_summary else "No public market data matched."}
"""

    system_prompt = """You are a senior Commercial Real Estate analyst.
Using the cross-source data summary below, generate 6-8 distinct, non-obvious, actionable insights.
You must use the specific numbers provided.
Categories: Market Activity | Geographic Shifts | Financial Risk | Sector Trends | Future Outlook | Capital Flow
Each insight must connect data from at LEAST two different sources.
Return ONLY a valid JSON array:
[{"category":"Market Activity","title":"Short Title","description":"2-3 sentences citing specific data points from the summary.","sources_used":["CRE Lending","News"],"confidence":"High"}]"""

    result = call_llm(system_prompt, context, temperature=0.4, max_tokens=3000)
    parsed = _parse_json(result)
    return parsed if isinstance(parsed, list) else []


def answer_query(query: str, context: dict) -> str:
    records = context.get("records", [])
    insights = context.get("insights", [])
    fmp = context.get("fmp_profiles", {})

    # Build compact context from records
    deal_sample = [
        {k: r[k] for k in ["lender","borrower","city_display","asset_class","loan_amount_usd","ltv_ratio","date","ai_sentiment","ai_summary"]
         if k in r and r[k] is not None}
        for r in records if r.get("source_type") == "structured" and r.get("lender")
    ][:15]

    article_sample = [
        {k: r[k] for k in ["source","title","city_display","asset_class","ai_sentiment","ai_summary","fmp_ticker","fmp_stock_price"]
         if k in r and r[k] is not None}
        for r in records if r.get("source_type") in ("rss","web_scrape")
    ][:10]

    housing_sample = [
        {k: r[k] for k in ["source","city_display","list_price_usd","price_per_sqft","year_built"]
         if k in r and r[k] is not None}
        for r in records if r.get("source_type") == "csv"
    ][:8]

    fmp_sample = [
        f"{name}: {p.get('ticker')} | sector={p.get('sector')} | price={p.get('price')} | mktCap={p.get('mkt_cap')}"
        for name, p in list(fmp.items())[:6]
    ]

    data_ctx = json.dumps({
        "deals":          deal_sample,
        "articles":       article_sample,
        "housing_market": housing_sample,
        "public_equities": fmp_sample,
        "ai_insights":    insights[:6],
    }, default=str, indent=2)

    system_prompt = f"""You are a Real Estate Intelligence Assistant. 
You have access to a unified dataset combining CRE lending deals (UK/EU), real estate news (RSS + web), US housing data, and public stock data from FMP.
Use ALL relevant data below to answer the question precisely, citing specific numbers where possible.
If you don't have enough data, say so honestly — don't hallucinate.

UNIFIED DATA:
{data_ctx[:7000]}"""

    return call_llm(system_prompt, query, temperature=0.3, max_tokens=1000) or "Sorry, couldn't process that query."
