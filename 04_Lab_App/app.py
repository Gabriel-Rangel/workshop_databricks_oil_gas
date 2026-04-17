"""
O&G Operations Hub — Streamlit Edition
=======================================
Plataforma integrada de monitoramento de operacoes de Oil & Gas.
3 paginas: Dashboard Producao, Modelo Preditivo, Genie Assistant.
PRIO brand identity.
"""

import os
import time
import logging
import requests
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from databricks.sdk.core import Config

# ---------------------------------------------------------------------------
# Page config — MUST be the very first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="O&G Operations Hub",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOGO = "workshop_databricks"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT_NAME", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
DASHBOARD_EMBED_URL = os.getenv("DASHBOARD_EMBED_URL", "")

cfg = Config()

CAMPOS = [
    "Frade", "Tubarao Martelo", "Polvo", "Albacora Leste", "Wahoo",
    "Peregrino", "Marlim", "Roncador", "Jubarte", "Buzios",
]

SAMPLE_QUESTIONS = [
    "Qual campo teve a maior producao total?",
    "Quanto gastamos em manutencao no campo de Frade?",
    "Quais pocos estao com BSW acima de 80%?",
    "Quantos pocos estao produzindo por bacia?",
    "Mostre os 5 maiores custos de manutencao",
]

# ---------------------------------------------------------------------------
# Databricks API helpers
# ---------------------------------------------------------------------------


def _get_auth_headers() -> dict:
    headers = cfg.authenticate()
    headers["Content-Type"] = "application/json"
    return headers


def _get_host() -> str:
    host = cfg.host or ""
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def call_serving_endpoint(features: dict) -> float:
    host = _get_host()
    headers = _get_auth_headers()
    if not SERVING_ENDPOINT:
        raise ValueError("SERVING_ENDPOINT_NAME nao configurado")
    resp = requests.post(
        f"{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations",
        headers=headers,
        json={"dataframe_records": [features]},
        timeout=120,
    )
    resp.raise_for_status()
    result = resp.json()
    predictions = result.get("predictions", result.get("outputs", []))
    if isinstance(predictions, list) and len(predictions) > 0:
        return float(predictions[0])
    return 0.0


def genie_start_conversation(question: str) -> dict:
    host = _get_host()
    headers = _get_auth_headers()
    if not GENIE_SPACE_ID:
        return {"error": "GENIE_SPACE_ID nao configurado"}
    resp = requests.post(
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
        headers=headers,
        json={"content": question},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    conversation_id = data.get("conversation_id", "")
    message_id = data.get("message_id", "")
    if not conversation_id or not message_id:
        return {"error": "Falha ao iniciar conversa", "raw": data}
    return _poll_genie_message(conversation_id, message_id)


def genie_follow_up(conversation_id: str, question: str) -> dict:
    host = _get_host()
    headers = _get_auth_headers()
    resp = requests.post(
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages",
        headers=headers,
        json={"content": question},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    message_id = data.get("id", "")
    if not message_id:
        return {"error": "Falha ao enviar mensagem", "raw": data}
    return _poll_genie_message(conversation_id, message_id)


def _poll_genie_message(conversation_id: str, message_id: str, max_wait: int = 90) -> dict:
    host = _get_host()
    headers = _get_auth_headers()
    url = (
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}"
        f"/conversations/{conversation_id}/messages/{message_id}"
    )
    for _ in range(max_wait):
        time.sleep(1)
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            continue
        data = resp.json()
        status = data.get("status", "")
        if status == "COMPLETED":
            return _parse_genie_response(data, conversation_id)
        elif status in ("FAILED", "CANCELLED"):
            return {"error": f"Genie falhou: {data.get('error', 'desconhecido')}", "conversation_id": conversation_id}
    return {"error": "Timeout aguardando resposta do Genie", "conversation_id": conversation_id}


def _parse_genie_response(data: dict, conversation_id: str) -> dict:
    result = {"conversation_id": conversation_id, "text": "", "sql": "", "table": None}
    for att in data.get("attachments", []):
        text_content = att.get("text", {}).get("content", "")
        if text_content:
            result["text"] += text_content + "\n"
        query_info = att.get("query", {})
        if query_info:
            result["sql"] = query_info.get("query", "")
            desc = query_info.get("description", "")
            if desc:
                result["text"] += desc + "\n"
    content = data.get("content", "")
    if content and not result["text"]:
        result["text"] = content
    return result


# ---------------------------------------------------------------------------
# Master CSS — PRIO brand dark theme
# ---------------------------------------------------------------------------

PRIO_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ── Base ── */
html, body, [data-testid="stAppViewContainer"], .stApp {
    background-color: #0d1b2a !important;
    font-family: 'Inter', sans-serif !important;
    color: #f0f4f8 !important;
}
[data-testid="stHeader"] { background: transparent !important; }
[data-testid="stToolbar"] { display: none !important; }
#MainMenu, footer { visibility: hidden !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #0d1b2a; }
::-webkit-scrollbar-thumb { background: #10afff; border-radius: 4px; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #0a1220 !important;
    border-right: 1px solid #1a3050 !important;
}
section[data-testid="stSidebar"] > div { padding-top: 0 !important; }

/* Sidebar collapse button */
[data-testid="stSidebarCollapsedControl"] button,
button[data-testid="baseButton-headerNoPadding"] {
    color: #10afff !important;
    background: rgba(16,175,255,0.08) !important;
    border: 1px solid rgba(16,175,255,0.2) !important;
    border-radius: 6px !important;
}

/* ── Sidebar nav radio ── */
[data-testid="stSidebar"] [data-testid="stRadio"] { margin-top: 0 !important; }
[data-testid="stSidebar"] [data-testid="stRadio"] > div {
    display: flex !important;
    flex-direction: column !important;
    gap: 2px !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label {
    display: flex !important;
    align-items: center !important;
    padding: 11px 16px !important;
    border-radius: 0 8px 8px 0 !important;
    border-left: 3px solid transparent !important;
    cursor: pointer !important;
    transition: all 0.15s ease !important;
    color: #8aa8c0 !important;
    font-weight: 500 !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 15px !important;
    background: transparent !important;
    margin: 0 !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    background: rgba(16,175,255,0.07) !important;
    color: #c0d0e0 !important;
    border-left-color: rgba(16,175,255,0.35) !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
    background: rgba(16,175,255,0.12) !important;
    color: #10afff !important;
    border-left-color: #10afff !important;
    font-weight: 700 !important;
}
/* Hide the radio dot and its baseweb wrapper */
[data-testid="stSidebar"] [data-testid="stRadio"] input[type="radio"],
[data-testid="stSidebar"] [data-baseweb="radio"] {
    display: none !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] [data-testid="stMarkdownContainer"] p {
    color: inherit !important;
    font-size: 15px !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: inherit !important;
}

/* ── Primary button (form submit) ── */
[data-testid="stFormSubmitButton"] > button {
    background: linear-gradient(135deg, #10afff 0%, #0693e3 100%) !important;
    color: #ffffff !important;
    border: none !important;
    font-weight: 700 !important;
    font-size: 15px !important;
    letter-spacing: 0.4px !important;
    border-radius: 8px !important;
    padding: 14px 24px !important;
    box-shadow: 0 4px 18px rgba(16,175,255,0.35) !important;
    transition: box-shadow 0.2s, transform 0.1s !important;
}
[data-testid="stFormSubmitButton"] > button:hover {
    box-shadow: 0 6px 24px rgba(16,175,255,0.5) !important;
    transform: translateY(-1px) !important;
}

/* ── Secondary buttons ── */
.stButton > button {
    background: rgba(17,34,54,0.8) !important;
    color: #a0b8cc !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 7px !important;
    font-size: 13px !important;
    text-align: left !important;
    padding: 9px 14px !important;
    transition: border-color 0.15s, color 0.15s, background 0.15s !important;
    font-family: 'Inter', sans-serif !important;
}
.stButton > button:hover {
    border-color: #10afff !important;
    color: #10afff !important;
    background: rgba(16,175,255,0.07) !important;
}

/* ── Reset button ── */
.btn-reset > button {
    background: rgba(255,107,107,0.08) !important;
    border-color: rgba(255,107,107,0.3) !important;
    color: #ff8080 !important;
}
.btn-reset > button:hover {
    background: rgba(255,107,107,0.15) !important;
    border-color: #ff6b6b !important;
    color: #ff6b6b !important;
}

/* ── Inputs ── */
[data-testid="stNumberInput"] input,
[data-testid="stTextInput"] input {
    background-color: #112236 !important;
    color: #f0f4f8 !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 7px !important;
    font-family: 'Inter', sans-serif !important;
}
[data-testid="stNumberInput"] input:focus,
[data-testid="stTextInput"] input:focus {
    border-color: #10afff !important;
    box-shadow: 0 0 0 2px rgba(16,175,255,0.15) !important;
}

/* ── Selectbox ── */
[data-testid="stSelectbox"] > div > div {
    background-color: #112236 !important;
    color: #f0f4f8 !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 7px !important;
}

/* ── Slider ── */
[data-testid="stSlider"] [role="slider"] {
    background-color: #10afff !important;
    border-color: #10afff !important;
}
[data-testid="stSlider"] [data-baseweb="slider"] [class*="TrackHighlight"] {
    background-color: #10afff !important;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
    background: #112236 !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 8px !important;
}
[data-testid="stExpander"] summary {
    color: #7a90a8 !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 13px !important;
}
[data-testid="stExpander"] summary:hover { color: #10afff !important; }

/* ── Code blocks ── */
[data-testid="stCode"] {
    background: #060f1c !important;
    border: 1px solid #1a3555 !important;
    border-radius: 8px !important;
}
/* Code inside expanders and chat messages */
[data-testid="stExpander"] [data-testid="stCode"],
[data-testid="stExpander"] pre,
[data-testid="stChatMessage"] [data-testid="stCode"],
[data-testid="stChatMessage"] pre {
    background: #060f1c !important;
    border: 1px solid #1a3555 !important;
    border-radius: 6px !important;
}
[data-testid="stExpander"] [data-testid="stCode"] *,
[data-testid="stExpander"] [data-testid="stCode"] code,
[data-testid="stChatMessage"] [data-testid="stCode"] *,
[data-testid="stChatMessage"] [data-testid="stCode"] code {
    color: #a8d8ff !important;
    background: transparent !important;
}
/* Standalone code snippets */
[data-testid="stCode"] code,
[data-testid="stCode"] * {
    color: #a8d8ff !important;
    background: transparent !important;
}

/* ── Chat messages — text readability fix ── */
[data-testid="stChatMessage"] {
    background: #0f2035 !important;
    border: 1px solid #1a3555 !important;
    border-radius: 12px !important;
    padding: 14px 18px !important;
    margin-bottom: 10px !important;
}
[data-testid="stChatMessage"] p,
[data-testid="stChatMessage"] span,
[data-testid="stChatMessage"] div,
[data-testid="stChatMessage"] .stMarkdown,
[data-testid="stChatMessage"] .stMarkdown p,
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p {
    color: #dde8f2 !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
    line-height: 1.6 !important;
}
/* User message — slightly different tint */
[data-testid="stChatMessage"][data-testid*="user"],
[data-testid="stChatMessage"]:has([aria-label="user avatar"]) {
    background: rgba(16,175,255,0.07) !important;
    border-color: rgba(16,175,255,0.2) !important;
}
/* Chat input */
[data-testid="stChatInput"] textarea {
    background: #112236 !important;
    color: #f0f4f8 !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 8px !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
}
[data-testid="stChatInput"] textarea:focus {
    border-color: #10afff !important;
    box-shadow: 0 0 0 2px rgba(16,175,255,0.15) !important;
}
[data-testid="stChatInput"] button {
    background: linear-gradient(135deg, #10afff, #0693e3) !important;
    border-radius: 6px !important;
}
[data-testid="stChatInput"] textarea::placeholder {
    color: #3a5a78 !important;
}

/* ── Form container ── */
[data-testid="stForm"] {
    background: rgba(17,34,54,0.5) !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 12px !important;
    padding: 20px !important;
}

/* ── Spinner ── */
[data-testid="stSpinner"] p { color: #10afff !important; }

/* ── Dividers ── */
hr { border-color: #1e3a5f !important; }

/* ── Labels ── */
label, [data-testid="stWidgetLabel"] p {
    color: #a0b0c0 !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    font-family: 'Inter', sans-serif !important;
}

/* ── Alerts ── */
[data-testid="stAlert"] {
    border-radius: 8px !important;
    font-family: 'Inter', sans-serif !important;
}
[data-testid="stAlert"] p {
    color: inherit !important;
    font-family: 'Inter', sans-serif !important;
}

/* ── Animations ── */
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(16px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes pulse-green {
    0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(0,208,132,0.5); }
    50%       { opacity: 0.85; box-shadow: 0 0 0 5px rgba(0,208,132,0); }
}
@keyframes pulse-cyan {
    0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(16,175,255,0.5); }
    50%       { opacity: 0.85; box-shadow: 0 0 0 5px rgba(16,175,255,0); }
}

/* ── KPI cards ── */
.kpi-row { display: flex; gap: 12px; margin-top: 16px; }
.kpi-card {
    flex: 1;
    background: #112236;
    border: 1px solid #1e3a5f;
    border-radius: 10px;
    padding: 16px 12px;
    text-align: center;
    animation: fadeInUp 0.4s ease forwards;
}
.kpi-card.kpi-primary {
    border-color: rgba(16,175,255,0.4);
    background: linear-gradient(135deg, rgba(16,175,255,0.1), rgba(6,147,227,0.05));
}
.kpi-card.kpi-positive {
    border-color: rgba(0,208,132,0.4);
    background: linear-gradient(135deg, rgba(0,208,132,0.1), rgba(0,208,132,0.03));
}
.kpi-card.kpi-negative {
    border-color: rgba(255,100,100,0.4);
    background: linear-gradient(135deg, rgba(255,100,100,0.1), rgba(255,100,100,0.03));
}
.kpi-label {
    font-size: 11px;
    color: #7a90a8;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 8px;
    font-family: 'Inter', sans-serif;
    font-weight: 500;
}
.kpi-value {
    font-size: 22px;
    font-weight: 700;
    color: #10afff;
    font-family: 'Inter', sans-serif;
    line-height: 1.1;
}
.kpi-value.kpi-neutral { color: #f0f4f8; }
.kpi-value.kpi-green   { color: #00d084; }
.kpi-value.kpi-red     { color: #ff6464; }

/* ── Section header ── */
.prio-section-header {
    background: linear-gradient(135deg, rgba(16,175,255,0.1), rgba(6,147,227,0.04));
    border-left: 3px solid #10afff;
    border-radius: 0 8px 8px 0;
    padding: 10px 16px;
    color: #f0f4f8 !important;
    font-weight: 700;
    font-size: 15px;
    margin-bottom: 18px;
    font-family: 'Inter', sans-serif;
}

/* ── App header ── */
.prio-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: linear-gradient(135deg, #06111e 0%, #0a1628 60%, #0d1e32 100%);
    border-bottom: 1px solid rgba(16,175,255,0.2);
    padding: 18px 28px 16px;
    margin: -60px -68px 28px;
    box-shadow: 0 2px 20px rgba(0,0,0,0.5);
}
.prio-header-left { display: flex; align-items: center; gap: 14px; }
.prio-icon { font-size: 32px; line-height: 1; }
.prio-title {
    margin: 0 0 4px;
    font-size: 22px;
    font-weight: 800;
    background: linear-gradient(90deg, #f0f4f8 0%, #10afff 70%, #0693e3 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-family: 'Inter', sans-serif;
    letter-spacing: -0.5px;
}
.prio-sub {
    margin: 0;
    font-size: 12px;
    color: #7a90a8;
    font-family: 'Inter', sans-serif;
    display: flex;
    align-items: center;
    gap: 10px;
}
.prio-badge-powered {
    background: rgba(16,175,255,0.1);
    border: 1px solid rgba(16,175,255,0.25);
    color: #10afff;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.3px;
}
.prio-header-right { display: flex; gap: 8px; align-items: center; }
.status-pill {
    display: flex;
    align-items: center;
    gap: 5px;
    padding: 4px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    font-family: 'Inter', sans-serif;
}
.status-pill.ok  { background: rgba(0,208,132,0.1); border: 1px solid rgba(0,208,132,0.3); color: #00d084; }
.status-pill.err { background: rgba(255,100,100,0.1); border: 1px solid rgba(255,100,100,0.3); color: #ff6464; }
.pulse-dot { width: 6px; height: 6px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
.ok  .pulse-dot { background: #00d084; animation: pulse-green 2s infinite; }
.err .pulse-dot { background: #ff6464; }

/* ── Dashboard placeholder ── */
.dash-placeholder {
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    padding: 80px 40px;
    background: rgba(17,34,54,0.5);
    border: 1px solid #1e3a5f;
    border-radius: 14px;
    text-align: center;
    margin: 20px auto;
    max-width: 640px;
}
.dash-icon-wrap {
    width: 96px; height: 96px; border-radius: 50%;
    background: linear-gradient(135deg, rgba(16,175,255,0.15), rgba(6,147,227,0.08));
    border: 1px solid rgba(16,175,255,0.3);
    display: flex; align-items: center; justify-content: center;
    font-size: 44px; margin-bottom: 24px;
}
.dash-title { font-size: 22px; font-weight: 700; color: #f0f4f8; margin-bottom: 6px; font-family: 'Inter', sans-serif; }
.dash-sub   { font-size: 14px; color: #7a90a8; margin-bottom: 24px; font-family: 'Inter', sans-serif; }
.dash-steps { display: flex; align-items: center; gap: 8px; font-size: 13px; font-family: 'Inter', sans-serif; flex-wrap: wrap; justify-content: center; }
.dash-step  { color: #a0b0c0; }
.dash-step.active { color: #10afff; font-weight: 600; }
.dash-arrow { color: #3a5070; }
</style>
"""

st.markdown(PRIO_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar — navigation + status
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("""
    <div style="
        padding: 24px 20px 20px;
        border-bottom: 1px solid #1a3050;
        margin-bottom: 8px;
    ">
        <div style="font-size:30px; margin-bottom:10px; line-height:1;">⛽</div>
        <div style="
            font-size:15px; font-weight:800;
            background: linear-gradient(90deg,#f0f4f8 0%,#10afff 100%);
            -webkit-background-clip:text; -webkit-text-fill-color:transparent;
            background-clip:text;
            font-family:Inter,sans-serif; letter-spacing:-0.3px; margin-bottom:4px;
        ">O&G Operations Hub</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="
        padding: 12px 20px 6px;
        font-size:10px; color:#5a8099;
        font-family:Inter,sans-serif;
        text-transform:uppercase; letter-spacing:1.2px; font-weight:600;
    ">Navegação</div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "",
        [
            "📊   Dashboard",
            "🤖   Modelo Preditivo",
            "💬   Genie Assistant",
        ],
        label_visibility="collapsed",
    )

    # Status section
    st.markdown("""
    <div style="
        margin-top: 32px;
        padding: 14px 20px 8px;
        border-top: 1px solid #1a3050;
        font-size:10px; color:#5a8099;
        font-family:Inter,sans-serif;
        text-transform:uppercase; letter-spacing:1.2px; font-weight:600;
        margin-bottom: 6px;
    ">Status</div>
    """, unsafe_allow_html=True)

    for label, val in [
        ("SQL Warehouse", WAREHOUSE_ID),
        ("Model Serving", SERVING_ENDPOINT),
        ("Genie Space", GENIE_SPACE_ID),
    ]:
        ok = bool(val)
        dot_color = "#00d084" if ok else "#ff6464"
        text_color = "#80a890" if ok else "#a07070"
        icon = "✓" if ok else "✗"
        st.markdown(f"""
        <div style="
            display:flex; align-items:center; gap:8px;
            padding: 5px 20px;
            font-family:Inter,sans-serif; font-size:12px; color:{text_color};
        ">
            <span style="
                width:6px; height:6px; border-radius:50%;
                background:{dot_color}; display:inline-block; flex-shrink:0;
            "></span>
            {icon}&nbsp;{label}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("""
    <div style="
        margin: 24px 16px 16px;
        padding: 14px 16px;
        background: rgba(255,54,33,0.07);
        border: 1px solid rgba(255,54,33,0.18);
        border-radius: 10px;
        display: flex;
        align-items: center;
        gap: 12px;
    ">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="flex-shrink:0;">
            <path d="M12 2L2 7.5V12l10 5.5L22 12V7.5L12 2z" fill="#FF3621"/>
            <path d="M2 12v4.5L12 22l10-5.5V12l-10 5.5L2 12z" fill="#FF3621" opacity="0.7"/>
        </svg>
        <div>
            <div style="font-size:10px; color:#a07060; font-family:Inter,sans-serif; letter-spacing:0.4px; margin-bottom:2px;">Powered by</div>
            <div style="font-size:14px; font-weight:700; color:#f0e8e6; font-family:Inter,sans-serif; letter-spacing:-0.2px;">Databricks</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# App header (top bar — visible on all pages)
# ---------------------------------------------------------------------------


def _status_pill(label: str, ok: bool) -> str:
    cls = "ok" if ok else "err"
    icon = "✓" if ok else "✗"
    return (
        f'<span class="status-pill {cls}">'
        f'<span class="pulse-dot"></span>{icon} {label}'
        f"</span>"
    )


HEADER_HTML = f"""
<div class="prio-header">
  <div class="prio-header-left">
    <span class="prio-icon">⛽</span>
    <div>
      <h1 class="prio-title">O&amp;G Operations Hub</h1>
      <p class="prio-sub">
        Plataforma Integrada de Monitoramento
        <span class="prio-badge-powered">⚡ Powered by Databricks</span>
      </p>
    </div>
  </div>
  <div class="prio-header-right">
    {_status_pill("SQL Warehouse", bool(WAREHOUSE_ID))}
    {_status_pill("Model Serving", bool(SERVING_ENDPOINT))}
    {_status_pill("Genie", bool(GENIE_SPACE_ID))}
  </div>
</div>
"""

st.markdown(HEADER_HTML, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

if "genie_history" not in st.session_state:
    st.session_state.genie_history = []
if "genie_conv_id" not in st.session_state:
    st.session_state.genie_conv_id = ""
if "last_sql" not in st.session_state:
    st.session_state.last_sql = ""
if "pending_question" not in st.session_state:
    st.session_state.pending_question = ""
if "prediction_result" not in st.session_state:
    st.session_state.prediction_result = None

# ---------------------------------------------------------------------------
# Page routing
# ---------------------------------------------------------------------------

# ===========================================================================
# PAGE 1 — Dashboard Producao
# ===========================================================================

if "Dashboard" in page:
    st.markdown('<div class="prio-section-header">📊 Dashboard Producao</div>', unsafe_allow_html=True)

    if DASHBOARD_EMBED_URL:
        components.iframe(DASHBOARD_EMBED_URL, height=820, scrolling=False)
    else:
        st.markdown("""
        <div class="dash-placeholder">
          <div class="dash-icon-wrap">📊</div>
          <div class="dash-title">Dashboard AI/BI</div>
          <div class="dash-sub">Embed do Painel Executivo de Producao</div>
          <div style="
            background: rgba(16,175,255,0.07);
            border: 1px solid rgba(16,175,255,0.2);
            border-radius: 8px;
            padding: 12px 20px;
            color: #a0b0c0;
            font-size: 13px;
            margin-bottom: 20px;
            font-family: Inter, sans-serif;
          ">
            ℹ️  Configure a variavel de ambiente
            <code style="color:#10afff; background:rgba(16,175,255,0.1); padding:2px 6px; border-radius:4px;">DASHBOARD_EMBED_URL</code>
            com a URL de embed do dashboard publicado.
          </div>
          <div class="dash-steps">
            <span class="dash-step">Dashboard</span>
            <span class="dash-arrow">→</span>
            <span class="dash-step">Share</span>
            <span class="dash-arrow">→</span>
            <span class="dash-step active">Embed &gt; Copy URL</span>
          </div>
        </div>
        """, unsafe_allow_html=True)

# ===========================================================================
# PAGE 2 — Modelo Preditivo
# ===========================================================================

elif "Modelo" in page:
    col_form, col_result = st.columns([5, 7], gap="large")

    # ── Left: input form ──────────────────────────────────────────────────
    with col_form:
        st.markdown('<div class="prio-section-header">⚙️ Parametros de Entrada</div>', unsafe_allow_html=True)

        with st.form("predict_form", clear_on_submit=False, border=False):
            campo = st.selectbox("Campo", CAMPOS, index=0)
            semana = st.slider("Semana do Ano", min_value=1, max_value=52, value=26)

            c1, c2 = st.columns(2)
            bsw = c1.number_input("BSW Medio (%)", min_value=0.0, max_value=100.0,
                                  value=45.0, step=0.1, format="%.1f")
            pressao = c2.number_input("Pressao Media (psi)", min_value=50.0, max_value=5000.0,
                                      value=1500.0, step=10.0, format="%.0f")

            c3, c4 = st.columns(2)
            temp = c3.number_input("Temperatura Media (°C)", min_value=30.0, max_value=150.0,
                                   value=75.0, step=1.0, format="%.0f")
            horas = c4.number_input("Horas Operacao Media", min_value=0.0, max_value=24.0,
                                    value=20.0, step=0.5, format="%.1f")

            c5, c6, c7 = st.columns(3)
            pocos = c5.number_input("Pocos Ativos", min_value=1, max_value=500, value=50, step=1)
            vol_ant = c6.number_input("Vol. Ant. (bbl)", min_value=0.0, value=50000.0,
                                      step=1000.0, format="%.0f")
            vol_media = c7.number_input("Media 4 Sem. (bbl)", min_value=0.0, value=48000.0,
                                        step=1000.0, format="%.0f")

            submitted = st.form_submit_button(
                "▶   Prever Producao",
                use_container_width=True,
                type="primary",
            )

    # ── Right: results ────────────────────────────────────────────────────
    with col_result:
        st.markdown('<div class="prio-section-header">📈 Resultado da Previsao</div>', unsafe_allow_html=True)

        if submitted:
            if not SERVING_ENDPOINT:
                st.warning("Model Serving endpoint nao configurado. Configure a variavel SERVING_ENDPOINT_NAME.")
            else:
                features = {
                    "semana": int(semana),
                    "bsw_medio": float(bsw),
                    "pressao_media": float(pressao),
                    "temperatura_media": float(temp),
                    "horas_operacao_media": float(horas),
                    "pocos_ativos": int(pocos),
                    "vol_semana_anterior": float(vol_ant),
                    "vol_media_4_semanas": float(vol_media),
                }
                with st.spinner("Consultando modelo..."):
                    try:
                        prediction = call_serving_endpoint(features)
                        st.session_state.prediction_result = {
                            "prediction": prediction,
                            "campo": campo,
                            "vol_ant": float(vol_ant),
                        }
                    except Exception as e:
                        st.error(f"Erro ao chamar o modelo: {e}")
                        st.session_state.prediction_result = None

        if st.session_state.prediction_result:
            res = st.session_state.prediction_result
            prediction = res["prediction"]
            vol_ant_val = res["vol_ant"]
            campo_val = res["campo"]

            delta_pct = ((prediction - vol_ant_val) / vol_ant_val * 100) if vol_ant_val else 0
            is_positive = prediction >= vol_ant_val

            # Gauge chart
            max_val = max(prediction * 2, 200000)
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=prediction,
                number={"suffix": " bbl", "font": {"size": 32, "color": "#10afff", "family": "Inter"}},
                delta={"reference": vol_ant_val, "relative": False, "suffix": " bbl",
                       "increasing": {"color": "#00d084"}, "decreasing": {"color": "#ff6464"}},
                title={"text": f"Producao Prevista — {campo_val}",
                       "font": {"size": 16, "color": "#a0b0c0", "family": "Inter"}},
                gauge={
                    "axis": {"range": [0, max_val], "tickcolor": "#3a5070",
                             "tickfont": {"color": "#7a90a8", "size": 11}},
                    "bar": {"color": "#10afff", "thickness": 0.25},
                    "bgcolor": "rgba(0,0,0,0)",
                    "borderwidth": 0,
                    "steps": [
                        {"range": [0, max_val * 0.3], "color": "rgba(255,100,100,0.15)"},
                        {"range": [max_val * 0.3, max_val * 0.7], "color": "rgba(255,203,0,0.12)"},
                        {"range": [max_val * 0.7, max_val], "color": "rgba(0,208,132,0.12)"},
                    ],
                    "threshold": {
                        "line": {"color": "#f0f4f8", "width": 2},
                        "thickness": 0.8,
                        "value": vol_ant_val,
                    },
                },
            ))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font={"color": "#f0f4f8", "family": "Inter"},
                height=300,
                margin={"l": 20, "r": 20, "t": 60, "b": 10},
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

            # KPI cards via custom HTML
            delta_color = "kpi-green" if is_positive else "kpi-red"
            delta_card_class = "kpi-positive" if is_positive else "kpi-negative"
            sign = "+" if delta_pct >= 0 else ""

            st.markdown(f"""
            <div class="kpi-row">
              <div class="kpi-card kpi-primary">
                <div class="kpi-label">Previsao</div>
                <div class="kpi-value">{prediction:,.0f} bbl</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-label">Sem. Anterior</div>
                <div class="kpi-value kpi-neutral">{vol_ant_val:,.0f} bbl</div>
              </div>
              <div class="kpi-card {delta_card_class}">
                <div class="kpi-label">Variacao</div>
                <div class="kpi-value {delta_color}">{sign}{delta_pct:.1f}%</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

        else:
            st.markdown("""
            <div style="
                text-align: center; padding: 60px 20px;
                font-family: Inter,sans-serif;
            ">
                <div style="font-size:48px; margin-bottom:16px; opacity:0.3">📈</div>
                <div style="font-size:14px; color:#4a6880;">
                    Preencha os parametros e clique em
                    <strong style="color:#6a8898">Prever Producao</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Model info expander
        with st.expander("ℹ️  Sobre o Modelo", expanded=(st.session_state.prediction_result is None)):
            col_i1, col_i2 = st.columns(2)
            col_i1.markdown(f"**Modelo**  \n`{SERVING_ENDPOINT or 'modelo_producao_decline@champion'}`")
            col_i1.markdown("**Algoritmo**  \nRandom Forest Regressor")
            col_i2.markdown("**Target**  \nProducao semanal de oleo (bbl)")
            col_i2.markdown("**Features**")
            st.code(
                "semana, bsw_medio, pressao_media, temperatura_media,\n"
                "horas_operacao_media, pocos_ativos,\n"
                "vol_semana_anterior, vol_media_4_semanas",
                language="text",
            )

# ===========================================================================
# PAGE 3 — Genie Assistant
# ===========================================================================

elif "Genie" in page:
    col_chat, col_side = st.columns([8, 4], gap="large")

    # ── Right sidebar panel ───────────────────────────────────────────────
    with col_side:
        st.markdown(
            '<div class="prio-section-header" style="font-size:13px;">💡 Perguntas Sugeridas</div>',
            unsafe_allow_html=True,
        )

        for q in SAMPLE_QUESTIONS:
            if st.button(q, key=f"sq_{q[:30]}", use_container_width=True):
                st.session_state.pending_question = q
                st.rerun()

        if st.session_state.last_sql:
            st.markdown(
                '<div class="prio-section-header" style="font-size:13px; margin-top:20px;">🗄️ SQL Gerado</div>',
                unsafe_allow_html=True,
            )
            st.code(st.session_state.last_sql, language="sql")

        if st.session_state.genie_history:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<div class="btn-reset">', unsafe_allow_html=True)
            if st.button("🔄  Nova Conversa", use_container_width=True, key="reset_chat"):
                st.session_state.genie_history = []
                st.session_state.genie_conv_id = ""
                st.session_state.last_sql = ""
                st.session_state.pending_question = ""
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    # ── Left: chat area ───────────────────────────────────────────────────
    with col_chat:
        st.markdown(
            '<div class="prio-section-header">🤖 Genie — Assistente de Dados</div>',
            unsafe_allow_html=True,
        )

        if not st.session_state.genie_history:
            with st.chat_message("assistant", avatar="🤖"):
                st.write(
                    "Ola! Sou o assistente de dados da operadora. "
                    "Faca perguntas sobre producao, pocos e manutencao em linguagem natural."
                )

        for msg in st.session_state.genie_history:
            avatar = "🤖" if msg["role"] == "assistant" else "👤"
            with st.chat_message(msg["role"], avatar=avatar):
                st.write(msg["text"])
                if msg.get("sql"):
                    with st.expander("Ver SQL gerado"):
                        st.code(msg["sql"], language="sql")

        # Determine question to send
        question_to_send = ""
        if st.session_state.pending_question:
            question_to_send = st.session_state.pending_question
            st.session_state.pending_question = ""

        if typed := st.chat_input("Digite sua pergunta sobre producao, pocos ou manutencao..."):
            question_to_send = typed

        if question_to_send:
            if not GENIE_SPACE_ID:
                st.error("Genie Space ID nao configurado. Configure a variavel GENIE_SPACE_ID.")
            else:
                st.session_state.genie_history.append(
                    {"role": "user", "text": question_to_send, "sql": ""}
                )

                with st.chat_message("user", avatar="👤"):
                    st.write(question_to_send)

                with st.chat_message("assistant", avatar="🤖"):
                    with st.spinner("Consultando Genie..."):
                        try:
                            if st.session_state.genie_conv_id:
                                result = genie_follow_up(
                                    st.session_state.genie_conv_id, question_to_send
                                )
                            else:
                                result = genie_start_conversation(question_to_send)

                            response_text = result.get("text", "").strip() or "Resposta recebida sem texto."
                            sql = result.get("sql", "")
                            st.session_state.genie_conv_id = result.get(
                                "conversation_id", st.session_state.genie_conv_id
                            )

                            if "error" in result:
                                response_text = f"Erro: {result['error']}"
                                sql = ""

                        except Exception as e:
                            logger.error(f"Erro no Genie: {e}")
                            response_text = f"Erro ao processar pergunta: {e}"
                            sql = ""

                    st.session_state.genie_history.append(
                        {"role": "assistant", "text": response_text, "sql": sql}
                    )
                    if sql:
                        st.session_state.last_sql = sql

                    st.write(response_text)
                    if sql:
                        with st.expander("Ver SQL gerado"):
                            st.code(sql, language="sql")
