import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st.error("Module manquant. Installez avec : pip install streamlit-autorefresh")
    st.stop()

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="Crypto ETL Real-Time",
    page_icon="",
    layout="wide"
)

# AUTO-REFRESH TOUTES LES 2 MINUTES (120 secondes)
count = st_autorefresh(interval=120 * 1000, limit=None, key="crypto_refresh")

# --- STYLE CSS ---
st.markdown("""
    <style>
    .stApp {
        background: radial-gradient(circle at top right, #1E2329, #0E1117);
        color: #FFFFFF;
    }
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 20px;
        border-radius: 15px;
        transition: transform 0.3s;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        border-color: #14F195;
    }
    .stButton>button {
        width: 100%;
        border-radius: 10px;
        background-color: #14F195;
        color: black;
        font-weight: bold;
        border: none;
    }
    .stDataFrame { border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 10px; }
    .refresh-indicator {
        background: #14F195;
        color: black;
        padding: 5px 12px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 12px;
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    </style>
    """, unsafe_allow_html=True)

# --- CONNEXION ---
engine = create_engine("mysql+pymysql://root:@localhost:3306/cryptodb")

# FONCTION SANS CACHE pour avoir les données fraîches à chaque refresh
def load_data():
    query = "SELECT * FROM crypto_prices_recent ORDER BY market_cap_rank ASC"
    return pd.read_sql(query, engine)

# --- BARRE LATÉRALE ---
with st.sidebar:
    st.image("https://cryptologos.cc/logos/bitcoin-btc-logo.png", width=60)
    st.title("Crypto Pipeline")
    st.markdown("---")
    st.success(" Système : Opérationnel")
    
    # Indicateur de refresh
    st.info(f" Refresh #{count}")
    st.caption(f"Prochain refresh dans ~2 min")
    
    with st.expander(" Détails Techniques"):
        st.write("**Orchestrateur:** Airflow")
        st.write("**Base:** MySQL (XAMPP)")
        st.write("**Fréquence:** 120s")
        st.write(f"**Heure actuelle:** {datetime.now().strftime('%H:%M:%S')}")
    
    if st.button(' ACTUALISATION MANUELLE'):
        st.rerun()

# --- EN-TÊTE ---
col_title, col_status = st.columns([4, 1])
with col_title:
    st.title(" Real-Time Crypto Monitoring")
    st.markdown("Visualisation dynamique du pipeline **Extract-Transform-Load**")
with col_status:
    st.markdown(f"<br><span class='refresh-indicator'>🟢 LIVE</span>", unsafe_allow_html=True)

st.markdown("---")

try:
    df = load_data()

    if df.empty:
        st.warning("En attente de données depuis Airflow...")
    else:
        # --- TOP 3 METRICS ---
        st.subheader(" Top Actifs par Market Cap")
        m_col1, m_col2, m_col3 = st.columns(3)
        
        icons = ["₿", "Ξ", "◎"]
        for i in range(min(3, len(df))):
            row = df.iloc[i]
            cols = [m_col1, m_col2, m_col3]
            cols[i].metric(
                label=f"{icons[i] if i<3 else ''} {row['id'].upper()}",
                value=f"${row['current_price']:,.2f}",
                delta=f"Rang #{row['market_cap_rank']}"
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # --- KPI CARDS ---
        kpi1, kpi2, kpi3 = st.columns(3)
        avg_price = df['current_price'].mean()
        total_cap = df['market_cap'].sum()
        
        last_dt = df['processed_at'].iloc[0]
        display_time = last_dt.strftime('%H:%M:%S') if hasattr(last_dt, 'strftime') else str(last_dt)

        kpi1.markdown(f"<div style='border-left: 4px solid #627EEA; padding-left:15px;'><h5>Prix Moyen</h5><h2>${avg_price:,.2f}</h2></div>", unsafe_allow_html=True)
        kpi2.markdown(f"<div style='border-left: 4px solid #14F195; padding-left:15px;'><h5>Cap. Totale (Top 50)</h5><h2>${total_cap/1e9:,.1f}B</h2></div>", unsafe_allow_html=True)
        kpi3.markdown(f"<div style='border-left: 4px solid #F7931A; padding-left:15px;'><h5>Dernier Load</h5><h2>{display_time}</h2></div>", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # --- TABS ---
        tab_viz, tab_data = st.tabs([" ANALYSE GRAPHIQUE", " EXPLORATEUR DE DONNÉES"])

        with tab_viz:
            st.subheader("Comparaison des Prix (Top 15)")
            chart_data = df.head(15).copy()
            st.bar_chart(data=chart_data, x='symbol', y='current_price', color='#14F195')

        with tab_data:
            st.subheader("Données brutes en sortie de pipeline")
            st.dataframe(df.style.format({
                'current_price': '${:,.2f}',
                'market_cap': '${:,.0f}'
            }), use_container_width=True)

        # --- FOOTER ---
        st.markdown("---")
        f1, f2 = st.columns(2)
        display_date = last_dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(last_dt, 'strftime') else str(last_dt)
        f1.caption(f" Cycle ETL : {display_date} | 🔄 Refresh #{count}")
        f2.markdown("<p style='text-align:right; color:#888; font-size:12px;'>Propulsé par Airflow & Streamlit</p>", unsafe_allow_html=True)

except Exception as e:
    st.error(f" Erreur de liaison base de données : {e}")
    st.info("Vérifiez que XAMPP est bien démarré sur le port 3306.")