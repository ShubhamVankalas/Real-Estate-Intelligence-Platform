import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from ai_utils import answer_query

st.set_page_config(
    page_title="RE Intelligence Platform",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background-color: #070d1a; color: #e2e8f0; }
h1,h2,h3,h4 { color: #f8fafc; font-weight: 600; }
.metric-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    padding: 24px 20px; border-radius: 16px;
    border: 1px solid #334155;
    transition: transform 0.2s, border-color 0.2s;
}
.metric-card:hover { transform: translateY(-4px); border-color: #3b82f6; }
.metric-val { font-size: 2rem; font-weight: 700; margin-top: 6px; }
.metric-label { font-size: 0.8rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
.insight-card {
    background: rgba(59,130,246,0.07);
    border-left: 4px solid #3b82f6;
    padding: 18px 20px; border-radius: 0 12px 12px 0;
    margin-bottom: 16px;
}
.insight-title { color: #60a5fa; font-weight: 600; font-size: 1.05rem; margin-bottom: 6px; }
.cat-badge {
    display: inline-block; background: #1e293b; color: #94a3b8;
    border: 1px solid #334155; border-radius: 9999px;
    font-size: 0.7rem; font-weight: 600; padding: 2px 10px; margin-bottom: 10px;
}
.source-badge {
    font-size: 0.75rem; color: #475569; margin-top: 8px;
}
.fmp-card {
    background: linear-gradient(135deg, #134e4a 0%, #0f172a 100%);
    padding: 14px 18px; border-radius: 12px; border: 1px solid #134e4a;
    margin-bottom: 10px;
}
.section-divider {
    border: none; border-top: 1px solid #1e293b; margin: 32px 0;
}
</style>
""", unsafe_allow_html=True)


def fix_text(t):
    if not isinstance(t, str): return t
    return t.replace('ú','£').replace('Ç','€').replace('Ã£','£').replace('\u00a3','£')

def safe_float(v):
    try: return float(v)
    except: return None


@st.cache_data
def load_data():
    try:
        with open('unified_dataset.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return None

data = load_data()

if not data:
    st.title("Real Estate Intelligence Platform")
    st.warning("⏳ Pipeline is still running. Please wait for `unified_dataset.json` to be generated, then refresh this page.")
    st.stop()

records    = data.get("records", [])
insights   = data.get("insights", [])
fmp        = data.get("fmp_profiles", {})
meta       = data.get("meta", {})
city_map   = data.get("city_map", {})

deal_recs    = [r for r in records if r.get("source_type") == "structured" and r.get("lender")]
article_recs = [r for r in records if r.get("source_type") in ("rss", "web_scrape")]
housing_recs = [r for r in records if r.get("source_type") == "csv"]

df_deals    = pd.DataFrame(deal_recs)    if deal_recs    else pd.DataFrame()
df_articles = pd.DataFrame(article_recs) if article_recs else pd.DataFrame()
df_housing  = pd.DataFrame(housing_recs) if housing_recs else pd.DataFrame()

CHART_BG = "rgba(0,0,0,0)"
CHART_COLORS = ['#3b82f6','#10b981','#f59e0b','#8b5cf6','#ec4899','#06b6d4','#f97316','#a3e635']


st.markdown("<h1 style='font-size:2.2rem;margin-bottom:4px;'>Real Estate Intelligence Platform</h1>", unsafe_allow_html=True)
st.markdown("<p style='color:#64748b;font-size:0.95rem;margin-top:0;'>Unified analytical dashboard combining CRE lending, news scraping, housing data, and public equities.</p>", unsafe_allow_html=True)

total_usd = sum(r.get("loan_amount_usd") or 0 for r in deal_recs)
avg_ltv   = [r.get("ltv_ratio") for r in deal_recs if r.get("ltv_ratio")]
avg_ltv_v = round(sum(avg_ltv)/len(avg_ltv), 1) if avg_ltv else 0
uniq_cities = len(set(r.get("city_key") for r in deal_recs if r.get("city_key")))

c1,c2,c3,c4,c5 = st.columns(5)
for col, label, val, color in [
    (c1, "CRE Loan Volume", f"${total_usd/1e9:.1f}B", "#10b981"),
    (c2, "Deals Tracked",  str(len(deal_recs)), "#3b82f6"),
    (c3, "Avg LTV",        f"{avg_ltv_v}%", "#f59e0b"),
    (c4, "News Articles",  str(len(article_recs)), "#8b5cf6"),
    (c5, "FMP Companies",  str(len(fmp)), "#06b6d4"),
]:
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-val" style="color:{color};">{val}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["Dashboard & Visuals", "Cross-Source Insights", "Public Market", "Query Assistant"])

with tab1:

    st.subheader("CRE Lending Analytics")
    r1c1, r1c2, r1c3 = st.columns(3)

    with r1c1:
        if not df_deals.empty and "asset_class" in df_deals.columns:
            ac_counts = df_deals["asset_class"].value_counts().reset_index()
            ac_counts.columns = ["Asset Class", "Count"]
            fig = px.pie(ac_counts, names="Asset Class", values="Count", hole=0.5,
                         color_discrete_sequence=CHART_COLORS)
            fig.update_layout(title="Deals by Asset Class", paper_bgcolor=CHART_BG,
                              plot_bgcolor=CHART_BG, font_color="#e2e8f0",
                              margin=dict(t=40,l=0,r=0,b=0), legend=dict(font=dict(size=11)))
            st.plotly_chart(fig, width="stretch")

    with r1c2:
        if not df_deals.empty and "loan_amount_usd" in df_deals.columns:
            df_vol = df_deals.dropna(subset=["asset_class","loan_amount_usd"])
            vol = df_vol.groupby("asset_class")["loan_amount_usd"].sum().reset_index()
            vol.columns = ["Asset Class", "Volume USD"]
            vol["Volume $B"] = (vol["Volume USD"]/1e9).round(2)
            fig = px.bar(vol.sort_values("Volume $B", ascending=True),
                         x="Volume $B", y="Asset Class", orientation="h",
                         color="Asset Class", color_discrete_sequence=CHART_COLORS)
            fig.update_layout(title="Loan Volume by Asset Class ($B)", paper_bgcolor=CHART_BG,
                              plot_bgcolor=CHART_BG, font_color="#e2e8f0",
                              margin=dict(t=40,l=0,r=0,b=0), showlegend=False)
            st.plotly_chart(fig, width="stretch")

    with r1c3:
        if not df_deals.empty and "country_region" in df_deals.columns:
            rg = df_deals["country_region"].value_counts().reset_index()
            rg.columns = ["Region","Count"]
            fig = px.pie(rg, names="Region", values="Count", hole=0.5,
                         color_discrete_sequence=["#3b82f6","#10b981","#f59e0b"])
            fig.update_layout(title="Deals by Region", paper_bgcolor=CHART_BG,
                              plot_bgcolor=CHART_BG, font_color="#e2e8f0",
                              margin=dict(t=40,l=0,r=0,b=0))
            st.plotly_chart(fig, width="stretch")



    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    st.subheader("Asset Class Gap: Media Attention vs Capital Flow")
    st.caption("Are news articles covering the right sectors? Where is media attention concentrated vs actual deal volume?")

    r3c1, r3c2 = st.columns([2,1])
    with r3c1:
        # Media mentions per asset class
        media_ac = {}
        for a in article_recs:
            for t in (a.get("ai_topics") or []):
                media_ac[t] = media_ac.get(t, 0) + 1

        # Deal count per asset class
        deal_ac = {}
        if not df_deals.empty and "asset_class" in df_deals.columns:
            deal_ac = df_deals["asset_class"].value_counts().to_dict()

        all_acs = sorted(set(list(media_ac.keys()) + list(deal_ac.keys())))
        if all_acs:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="Media Articles",
                x=all_acs,
                y=[media_ac.get(ac, 0) for ac in all_acs],
                marker_color="#3b82f6",
            ))
            fig.add_trace(go.Bar(
                name="CRE Deals",
                x=all_acs,
                y=[deal_ac.get(ac, 0) for ac in all_acs],
                marker_color="#10b981",
            ))
            fig.update_layout(
                barmode="group", title="Media Articles vs CRE Deals by Asset Class",
                paper_bgcolor=CHART_BG, plot_bgcolor="rgba(15,23,42,0.5)",
                font_color="#e2e8f0", margin=dict(t=40,l=0,r=0,b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig, width="stretch")

    with r3c2:
        st.markdown("""
        <div class="insight-card" style="margin-top:30px;">
            <div class="insight-title">The Coverage Gap</div>
            <div style="color:#94a3b8;font-size:0.9rem;line-height:1.7;">
            <b style="color:#3b82f6;">Blue bars:</b> How often each asset class appears in scraped news (JLL, Altus, Property Week).<br><br>
            <b style="color:#10b981;">Green bars:</b> Actual number of CRE lending deals in each class.<br><br>
            <b style="color:#f59e0b;">Gaps reveal:</b> Where media over-hypes sectors with little real capital activity, and where undercovered markets may hold opportunity.
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    st.subheader("Institutional Capital & Deal Fluidity")
    
    if not df_deals.empty and "loan_amount_usd" in df_deals.columns:
        df_d = df_deals.dropna(subset=["loan_amount_usd","country_region"])
        fig = px.histogram(df_d, x="loan_amount_usd", color="country_region",
                           nbins=25, barmode="group", opacity=0.85,
                           color_discrete_map={"UK":"#3b82f6","Continental Europe":"#10b981"},
                           labels={"loan_amount_usd":"Loan Amount (USD)", "country_region":"Region"})
        fig.update_layout(title="CRE Loan Size Distribution (USD)", paper_bgcolor=CHART_BG,
                          plot_bgcolor="rgba(15,23,42,0.5)", font_color="#e2e8f0",
                          margin=dict(t=40,l=0,r=0,b=0))
        st.plotly_chart(fig, width="stretch")

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.subheader("Geographic Deal Activity")
    map_rows = []
    for r in deal_recs:
        if r.get("lat") and r.get("lon"):
            map_rows.append({
                "City": r.get("city_display","?"),
                "lat": r["lat"], "lon": r["lon"],
                "Asset Class": r.get("asset_class","?"),
                "Loan $M": round((r.get("loan_amount_usd") or 0)/1e6, 1),
                "LTV": r.get("ltv_ratio"),
                "Lender": r.get("lender","?"),
            })
    if map_rows:
        df_map = pd.DataFrame(map_rows)
        fig = px.scatter_map(
            df_map, lat="lat", lon="lon",
            color="Asset Class", size="Loan $M",
            hover_name="City",
            hover_data={"Lender":True,"LTV":True,"Loan $M":True,"lat":False,"lon":False},
            zoom=4, height=450,
            color_discrete_sequence=CHART_COLORS,
            size_max=30,
        )
        fig.update_layout(
            map_style="carto-darkmatter",
            paper_bgcolor=CHART_BG, margin=dict(t=0,l=0,r=0,b=0),
        )
        st.plotly_chart(fig, width="stretch")


with tab2:
    st.header("Cross-Source Insights")
    st.markdown("Derived analytical reports across CRE lending, news articles, housing data, and public equity datasets.")

    if insights:
        cat_map = {}
        for ins in insights:
            cat = ins.get("category", "General")
            cat_map.setdefault(cat, []).append(ins)

        cat_colors = {
            "Market Activity":"#3b82f6", "Geographic Shifts":"#10b981",
            "Financial Risk":"#ef4444", "Sector Trends":"#f59e0b",
            "Future Outlook":"#8b5cf6", "Capital Flow":"#06b6d4", "General":"#94a3b8"
        }
        for cat, items in cat_map.items():
            color = cat_colors.get(cat, "#94a3b8")
            st.markdown(f"<h4 style='color:{color};margin-top:24px;'>{cat}</h4>", unsafe_allow_html=True)
            cols = st.columns(min(len(items), 2))
            for i, ins in enumerate(items):
                with cols[i % 2]:
                    title = fix_text(ins.get("title",""))
                    desc  = fix_text(ins.get("description",""))
                    srcs  = ", ".join(ins.get("sources_used",[]))
                    conf  = ins.get("confidence","")
                    conf_color = "#10b981" if conf=="High" else "#f59e0b"
                    st.markdown(f"""
                    <div class="insight-card">
                        <div class="cat-badge">{cat}</div>
                        <div class="insight-title">{title}</div>
                        <div style="color:#cbd5e1;line-height:1.65;font-size:0.92rem;">{desc}</div>
                        <div class="source-badge">
                            <b>Sources:</b> {srcs} &nbsp;·&nbsp;
                            <span style="color:{conf_color};">● {conf} confidence</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
    else:
        st.info("No insights generated yet. Run pipeline.py first.")


with tab3:
    st.header("Public Real Estate Market")
    st.markdown("Company profiles queried dynamically via Financial Modeling Prep API based on entity extraction from textual sources.")

    if fmp:
        fmp_rows = []
        for name, p in fmp.items():
            fmp_rows.append({
                "Company": name,
                "Ticker": p.get("ticker"),
                "Exchange": p.get("exchange"),
                "Sector": p.get("sector"),
                "Industry": p.get("industry"),
                "Price ($)": p.get("price"),
                "Mkt Cap ($B)": round((p.get("mkt_cap") or 0)/1e9, 2),
                "Country": p.get("country"),
            })
        df_fmp = pd.DataFrame(fmp_rows)

        # Summary cards
        cols = st.columns(min(len(fmp_rows), 4))
        for i, row in enumerate(fmp_rows[:4]):
            with cols[i]:
                st.markdown(f"""
                <div class="fmp-card">
                    <b style="color:#10b981;">{row['Ticker']}</b> &nbsp;
                    <span style="color:#64748b;font-size:0.8rem;">{row['Exchange']}</span><br>
                    <span style="font-size:0.85rem;color:#e2e8f0;">{row['Company']}</span><br>
                    <span style="color:#f59e0b;font-size:1.2rem;font-weight:700;">${row['Price ($)']}</span><br>
                    <span style="color:#64748b;font-size:0.8rem;">Mkt Cap ${row['Mkt Cap ($B)']}B · {row['Sector']}</span>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.dataframe(df_fmp, width="stretch", hide_index=True)

        # Market cap chart
        if len(fmp_rows) >= 2:
            fig = px.bar(
                df_fmp.sort_values("Mkt Cap ($B)", ascending=False),
                x="Ticker", y="Mkt Cap ($B)", color="Sector",
                color_discrete_sequence=CHART_COLORS,
            )
            fig.update_layout(title="Market Cap by Company (FMP)", paper_bgcolor=CHART_BG,
                              plot_bgcolor="rgba(15,23,42,0.5)", font_color="#e2e8f0")
            st.plotly_chart(fig, width="stretch")
    else:
        st.info("No FMP profiles matched. The pipeline extracts company names from articles and queries FMP. If no articles had recognisable public company names, this section stays empty.")


with tab4:
    st.header("Intelligence Assistant")
    st.markdown("Interactive query engine connected to the unified dataset.")

    # seed suggestions
    st.markdown("""
    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;">
    <span style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:6px 14px;font-size:0.85rem;color:#94a3b8;">What kind of investments are happening in London?</span>
    <span style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:6px 14px;font-size:0.85rem;color:#94a3b8;">Which cities have the highest LTV deals?</span>
    <span style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:6px 14px;font-size:0.85rem;color:#94a3b8;">What does news sentiment tell us about market risk?</span>
    <span style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:6px 14px;font-size:0.85rem;color:#94a3b8;">How do US home prices compare to UK institutional deals?</span>
    </div>
    """, unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    chat_container = st.container(height=520, border=True)
    with chat_container:
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    if prompt := st.chat_input("Ask about any data in the unified dataset..."):
        st.session_state.messages.append({"role":"user","content":prompt})
        with chat_container:
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                with st.spinner("Querying unified dataset..."):
                    ctx = {
                        "records": records,
                        "insights": insights,
                        "fmp_profiles": fmp,
                    }
                    resp = answer_query("User Query: " + prompt, ctx)
                    resp = fix_text(resp)
                    st.markdown(resp)
                    st.session_state.messages.append({"role":"assistant","content":resp})
