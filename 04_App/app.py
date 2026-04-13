"""
O&G Operations Hub - Databricks App
====================================
Aplicacao Dash para monitoramento de operacoes de Oil & Gas.
4 abas: Mapa Operacional, Dashboard Producao, Modelo Preditivo, Genie Assistant.
"""

import os
import json
import time
import logging
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import dash
from dash import dcc, html, dash_table, callback_context, no_update
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

from databricks.sdk.core import Config

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CATALOGO = "workshop_databricks"
SCHEMA = os.getenv("SCHEMA", "gabriel_rangel")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT_NAME", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
DASHBOARD_EMBED_URL = os.getenv("DASHBOARD_EMBED_URL", "")

# Databricks SDK Config para autenticacao
cfg = Config()

# Campos disponiveis
CAMPOS = [
    "Frade", "Tubarao Martelo", "Polvo", "Albacora Leste", "Wahoo",
    "Peregrino", "Marlim", "Roncador", "Jubarte", "Buzios",
]

# Cores por status de poco
STATUS_COLORS = {
    "Produzindo": "#2ecc71",
    "Parado": "#e74c3c",
    "Manutencao": "#f1c40f",
    "Abandonado": "#95a5a6",
    "Completacao": "#3498db",
}

# ---------------------------------------------------------------------------
# Funcoes de acesso a dados
# ---------------------------------------------------------------------------


def _get_auth_headers():
    """Retorna headers de autenticacao usando Databricks SDK Config."""
    headers = cfg.authenticate()
    headers["Content-Type"] = "application/json"
    return headers


def _get_host():
    """Retorna o host do workspace com https://."""
    host = cfg.host
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def execute_sql(query: str) -> pd.DataFrame:
    """Executa uma query SQL via Databricks SQL Statement API e retorna DataFrame."""
    host = _get_host()
    headers = _get_auth_headers()

    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": query,
        "wait_timeout": "60s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    try:
        resp = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers=headers,
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status", {}).get("state", "")
        if status == "FAILED":
            error_msg = data.get("status", {}).get("error", {}).get("message", "Erro desconhecido")
            logger.error(f"SQL falhou: {error_msg}")
            return pd.DataFrame()

        # Aguardar se PENDING/RUNNING
        statement_id = data.get("statement_id")
        while status in ("PENDING", "RUNNING"):
            time.sleep(1)
            resp = requests.get(
                f"{host}/api/2.0/sql/statements/{statement_id}",
                headers=headers,
                timeout=30,
            )
            data = resp.json()
            status = data.get("status", {}).get("state", "")

        if status != "SUCCEEDED":
            logger.error(f"SQL terminou com status: {status}")
            return pd.DataFrame()

        manifest = data.get("manifest", {})
        columns = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]
        result = data.get("result", {})
        rows = result.get("data_array", [])

        if not columns or not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows, columns=columns)

    except Exception as e:
        logger.error(f"Erro ao executar SQL: {e}")
        return pd.DataFrame()


def load_pocos() -> pd.DataFrame:
    """Carrega dados de pocos da tabela silver_pocos."""
    query = f"""
    SELECT id_poco, codigo_sap, nome_poco, campo, bacia, tipo_poco,
           metodo_elevacao, profundidade_m, latitude, longitude, status
    FROM {CATALOGO}.{SCHEMA}.silver_pocos
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    df = execute_sql(query)
    if not df.empty:
        for col in ["latitude", "longitude", "profundidade_m"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def call_serving_endpoint(features: dict) -> float:
    """Chama o Model Serving endpoint para previsao de producao."""
    host = _get_host()
    headers = _get_auth_headers()

    endpoint_name = SERVING_ENDPOINT
    if not endpoint_name:
        raise ValueError("SERVING_ENDPOINT_NAME nao configurado")

    payload = {
        "dataframe_records": [features]
    }

    resp = requests.post(
        f"{host}/serving-endpoints/{endpoint_name}/invocations",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()

    # O retorno pode ter diferentes formatos
    predictions = result.get("predictions", result.get("outputs", []))
    if isinstance(predictions, list) and len(predictions) > 0:
        return float(predictions[0])
    return 0.0


# ---------------------------------------------------------------------------
# Genie API helpers
# ---------------------------------------------------------------------------


def genie_start_conversation(question: str) -> dict:
    """Inicia uma conversa com o Genie e retorna a resposta."""
    host = _get_host()
    headers = _get_auth_headers()

    if not GENIE_SPACE_ID:
        return {"error": "GENIE_SPACE_ID nao configurado"}

    # Iniciar conversa
    payload = {"content": question}
    resp = requests.post(
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
        headers=headers,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    conversation_id = data.get("conversation_id", "")
    message_id = data.get("message_id", "")

    if not conversation_id or not message_id:
        return {"error": "Falha ao iniciar conversa", "raw": data}

    # Polling para obter resultado
    return _poll_genie_message(conversation_id, message_id)


def genie_follow_up(conversation_id: str, question: str) -> dict:
    """Envia uma mensagem de follow-up em uma conversa existente."""
    host = _get_host()
    headers = _get_auth_headers()

    payload = {"content": question}
    resp = requests.post(
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages",
        headers=headers,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    message_id = data.get("id", "")
    if not message_id:
        return {"error": "Falha ao enviar mensagem", "raw": data}

    return _poll_genie_message(conversation_id, message_id)


def _poll_genie_message(conversation_id: str, message_id: str, max_wait: int = 90) -> dict:
    """Faz polling ate a mensagem do Genie estar completa."""
    host = _get_host()
    headers = _get_auth_headers()

    url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"

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
            error_msg = data.get("error", "Erro desconhecido")
            return {"error": f"Genie falhou: {error_msg}", "conversation_id": conversation_id}

    return {"error": "Timeout aguardando resposta do Genie", "conversation_id": conversation_id}


def _parse_genie_response(data: dict, conversation_id: str) -> dict:
    """Extrai texto, SQL e resultados da resposta do Genie."""
    result = {"conversation_id": conversation_id, "text": "", "sql": "", "table": None}

    attachments = data.get("attachments", [])
    for att in attachments:
        # Texto da resposta
        text_content = att.get("text", {}).get("content", "")
        if text_content:
            result["text"] += text_content + "\n"

        # Query SQL gerada
        query_info = att.get("query", {})
        if query_info:
            result["sql"] = query_info.get("query", "")
            # Descricao da query
            desc = query_info.get("description", "")
            if desc:
                result["text"] += desc + "\n"

    # Tambem pode ter texto direto no campo "content" (respostas de texto puro)
    content = data.get("content", "")
    if content and not result["text"]:
        result["text"] = content

    return result


def genie_get_query_result(conversation_id: str, message_id: str, attachment_id: str) -> pd.DataFrame:
    """Busca resultados de uma query do Genie."""
    host = _get_host()
    headers = _get_auth_headers()

    url = (
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}"
        f"/conversations/{conversation_id}"
        f"/messages/{message_id}"
        f"/query-result/{attachment_id}"
    )

    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        return pd.DataFrame()

    data = resp.json()
    columns = [col["name"] for col in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = data.get("result", {}).get("data_array", [])

    if not columns or not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=columns)


# ---------------------------------------------------------------------------
# Inicializacao do Dash
# ---------------------------------------------------------------------------

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY, dbc.icons.FONT_AWESOME],
    title="O&G Operations Hub",
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

server = app.server

# ---------------------------------------------------------------------------
# Estilos customizados
# ---------------------------------------------------------------------------

CARD_STYLE = {
    "backgroundColor": "#1a1a2e",
    "border": "1px solid #16213e",
    "borderRadius": "10px",
}

TAB_STYLE = {
    "backgroundColor": "#0f0f23",
    "borderColor": "#16213e",
    "color": "#8892b0",
    "padding": "12px 24px",
    "fontWeight": "500",
}

TAB_SELECTED_STYLE = {
    "backgroundColor": "#1a1a2e",
    "borderColor": "#00d4ff",
    "borderTop": "3px solid #00d4ff",
    "color": "#00d4ff",
    "padding": "12px 24px",
    "fontWeight": "600",
}

HEADER_STYLE = {
    "background": "linear-gradient(135deg, #0f0f23 0%, #1a1a2e 50%, #16213e 100%)",
    "padding": "20px 30px",
    "borderBottom": "2px solid #00d4ff",
    "marginBottom": "0px",
}


# ---------------------------------------------------------------------------
# Layout: Header
# ---------------------------------------------------------------------------

header = html.Div(
    style=HEADER_STYLE,
    children=[
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H2(
                            [
                                html.I(className="fas fa-oil-well me-3", style={"color": "#00d4ff"}),
                                "O&G Operations Hub",
                            ],
                            style={"color": "#e6f1ff", "marginBottom": "4px", "fontWeight": "700"},
                        ),
                        html.P(
                            f"Plataforma Integrada de Monitoramento | {CATALOGO}.{SCHEMA}",
                            style={"color": "#8892b0", "marginBottom": "0", "fontSize": "14px"},
                        ),
                    ],
                    width=8,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dbc.Badge(
                                    [html.I(className="fas fa-database me-1"), "SQL Warehouse"],
                                    color="success" if WAREHOUSE_ID else "danger",
                                    className="me-2",
                                    pill=True,
                                ),
                                dbc.Badge(
                                    [html.I(className="fas fa-brain me-1"), "Model Serving"],
                                    color="success" if SERVING_ENDPOINT else "danger",
                                    className="me-2",
                                    pill=True,
                                ),
                                dbc.Badge(
                                    [html.I(className="fas fa-robot me-1"), "Genie"],
                                    color="success" if GENIE_SPACE_ID else "danger",
                                    pill=True,
                                ),
                            ],
                            style={"textAlign": "right", "paddingTop": "8px"},
                        )
                    ],
                    width=4,
                ),
            ],
            align="center",
        )
    ],
)


# ---------------------------------------------------------------------------
# Tab 1: Mapa Operacional
# ---------------------------------------------------------------------------

tab_mapa = dbc.Container(
    fluid=True,
    className="py-3",
    children=[
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6(
                                        [html.I(className="fas fa-filter me-2"), "Filtros"],
                                        className="text-info mb-3",
                                    ),
                                    html.Label("Campo", className="text-light mb-1", style={"fontSize": "13px"}),
                                    dcc.Dropdown(
                                        id="mapa-campo-filter",
                                        options=[{"label": "Todos os Campos", "value": "Todos"}]
                                        + [{"label": c, "value": c} for c in CAMPOS],
                                        value="Todos",
                                        clearable=False,
                                        style={"backgroundColor": "#16213e", "color": "#000"},
                                        className="mb-3",
                                    ),
                                    html.Hr(style={"borderColor": "#16213e"}),
                                    html.H6("Legenda", className="text-info mb-2"),
                                    *[
                                        html.Div(
                                            [
                                                html.Span(
                                                    "\u25CF ",
                                                    style={"color": color, "fontSize": "18px"},
                                                ),
                                                html.Span(status, style={"color": "#ccd6f6", "fontSize": "13px"}),
                                            ],
                                            className="mb-1",
                                        )
                                        for status, color in STATUS_COLORS.items()
                                    ],
                                    html.Hr(style={"borderColor": "#16213e"}),
                                    html.Div(id="mapa-stats", className="mt-2"),
                                ]
                            ),
                            style=CARD_STYLE,
                        )
                    ],
                    width=3,
                ),
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    dcc.Loading(
                                        dcc.Graph(
                                            id="mapa-pocos",
                                            style={"height": "75vh"},
                                            config={"displayModeBar": True, "scrollZoom": True},
                                        ),
                                        type="circle",
                                        color="#00d4ff",
                                    )
                                ]
                            ),
                            style=CARD_STYLE,
                        )
                    ],
                    width=9,
                ),
            ]
        ),
    ],
)


# ---------------------------------------------------------------------------
# Tab 2: Dashboard Producao
# ---------------------------------------------------------------------------

if DASHBOARD_EMBED_URL:
    tab_dashboard_content = html.Iframe(
        src=DASHBOARD_EMBED_URL,
        style={
            "width": "100%",
            "height": "85vh",
            "border": "none",
            "borderRadius": "10px",
        },
    )
else:
    tab_dashboard_content = dbc.Container(
        fluid=True,
        className="py-5",
        children=[
            dbc.Row(
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.Div(
                                    [
                                        html.I(
                                            className="fas fa-chart-line",
                                            style={"fontSize": "64px", "color": "#00d4ff", "opacity": "0.5"},
                                        ),
                                        html.H4(
                                            "Dashboard AI/BI",
                                            className="text-light mt-4 mb-3",
                                        ),
                                        html.P(
                                            "O dashboard AI/BI sera exibido aqui quando a URL de embed for configurada.",
                                            className="text-muted mb-3",
                                        ),
                                        html.P(
                                            [
                                                "Configure a variavel de ambiente ",
                                                html.Code("DASHBOARD_EMBED_URL", style={"color": "#00d4ff"}),
                                                " com a URL do dashboard publicado.",
                                            ],
                                            className="text-muted",
                                            style={"fontSize": "13px"},
                                        ),
                                        html.Hr(style={"borderColor": "#16213e", "width": "50%", "margin": "20px auto"}),
                                        html.P(
                                            "Para obter a URL: Dashboard > Share > Embed > Copy Embed URL",
                                            className="text-muted",
                                            style={"fontSize": "12px"},
                                        ),
                                    ],
                                    style={"textAlign": "center", "padding": "60px 20px"},
                                )
                            ]
                        ),
                        style=CARD_STYLE,
                    ),
                    width=8,
                    className="mx-auto",
                ),
            )
        ],
    )

tab_dashboard = dbc.Container(fluid=True, className="py-3", children=[tab_dashboard_content])


# ---------------------------------------------------------------------------
# Tab 3: Modelo Preditivo
# ---------------------------------------------------------------------------

tab_modelo = dbc.Container(
    fluid=True,
    className="py-3",
    children=[
        dbc.Row(
            [
                # Formulario de entrada
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5(
                                        [html.I(className="fas fa-sliders-h me-2"), "Parametros de Entrada"],
                                        className="text-info mb-4",
                                    ),
                                    # Campo
                                    html.Label("Campo", className="text-light mb-1", style={"fontSize": "13px"}),
                                    dcc.Dropdown(
                                        id="modelo-campo",
                                        options=[{"label": c, "value": c} for c in CAMPOS],
                                        value="Frade",
                                        clearable=False,
                                        style={"backgroundColor": "#16213e", "color": "#000"},
                                        className="mb-3",
                                    ),
                                    # Semana
                                    html.Label("Semana do Ano", className="text-light mb-1", style={"fontSize": "13px"}),
                                    dcc.Slider(
                                        id="modelo-semana",
                                        min=1, max=52, step=1, value=26,
                                        marks={1: "1", 13: "13", 26: "26", 39: "39", 52: "52"},
                                        tooltip={"placement": "bottom", "always_visible": True},
                                        className="mb-3",
                                    ),
                                    # BSW medio
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    html.Label("BSW Medio (%)", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-bsw", type="number", value=45.0,
                                                        min=0, max=100, step=0.1,
                                                        className="bg-dark text-light",
                                                    ),
                                                ],
                                                width=6,
                                            ),
                                            dbc.Col(
                                                [
                                                    html.Label("Pressao Media (psi)", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-pressao", type="number", value=1500.0,
                                                        min=50, max=5000, step=10,
                                                        className="bg-dark text-light",
                                                    ),
                                                ],
                                                width=6,
                                            ),
                                        ],
                                        className="mb-3",
                                    ),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    html.Label("Temperatura Media (C)", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-temp", type="number", value=75.0,
                                                        min=30, max=150, step=1,
                                                        className="bg-dark text-light",
                                                    ),
                                                ],
                                                width=6,
                                            ),
                                            dbc.Col(
                                                [
                                                    html.Label("Horas Operacao Media", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-horas", type="number", value=20.0,
                                                        min=0, max=24, step=0.5,
                                                        className="bg-dark text-light",
                                                    ),
                                                ],
                                                width=6,
                                            ),
                                        ],
                                        className="mb-3",
                                    ),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    html.Label("Pocos Ativos", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-pocos", type="number", value=50,
                                                        min=1, max=500, step=1,
                                                        className="bg-dark text-light",
                                                    ),
                                                ],
                                                width=4,
                                            ),
                                            dbc.Col(
                                                [
                                                    html.Label("Vol. Sem. Anterior (bbl)", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-vol-ant", type="number", value=50000.0,
                                                        min=0, step=1000,
                                                        className="bg-dark text-light",
                                                    ),
                                                ],
                                                width=4,
                                            ),
                                            dbc.Col(
                                                [
                                                    html.Label("Media 4 Sem. (bbl)", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-vol-media", type="number", value=48000.0,
                                                        min=0, step=1000,
                                                        className="bg-dark text-light",
                                                    ),
                                                ],
                                                width=4,
                                            ),
                                        ],
                                        className="mb-4",
                                    ),
                                    dbc.Button(
                                        [html.I(className="fas fa-play me-2"), "Prever Producao"],
                                        id="modelo-predict-btn",
                                        color="info",
                                        size="lg",
                                        className="w-100",
                                        n_clicks=0,
                                    ),
                                ]
                            ),
                            style=CARD_STYLE,
                        )
                    ],
                    width=5,
                ),
                # Resultado
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5(
                                        [html.I(className="fas fa-chart-bar me-2"), "Resultado da Previsao"],
                                        className="text-info mb-4",
                                    ),
                                    dcc.Loading(
                                        html.Div(id="modelo-resultado"),
                                        type="circle",
                                        color="#00d4ff",
                                    ),
                                ]
                            ),
                            style=CARD_STYLE,
                        ),
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6(
                                        [html.I(className="fas fa-info-circle me-2"), "Sobre o Modelo"],
                                        className="text-info mb-3",
                                    ),
                                    html.P(
                                        f"Modelo: {CATALOGO}.{SCHEMA}.modelo_producao_decline@champion",
                                        className="text-muted mb-1",
                                        style={"fontSize": "12px"},
                                    ),
                                    html.P(
                                        "Algoritmo: Random Forest Regressor",
                                        className="text-muted mb-1",
                                        style={"fontSize": "12px"},
                                    ),
                                    html.P(
                                        "Target: Producao semanal de oleo (bbl)",
                                        className="text-muted mb-1",
                                        style={"fontSize": "12px"},
                                    ),
                                    html.P(
                                        [
                                            "Features: semana, bsw_medio, pressao_media, temperatura_media, ",
                                            "horas_operacao_media, pocos_ativos, vol_semana_anterior, vol_media_4_semanas",
                                        ],
                                        className="text-muted mb-0",
                                        style={"fontSize": "12px"},
                                    ),
                                ]
                            ),
                            style={**CARD_STYLE, "marginTop": "15px"},
                        ),
                    ],
                    width=7,
                ),
            ]
        ),
    ],
)


# ---------------------------------------------------------------------------
# Tab 4: Genie Assistant
# ---------------------------------------------------------------------------

SAMPLE_QUESTIONS = [
    "Qual campo teve a maior producao total?",
    "Quanto gastamos em manutencao no campo de Frade?",
    "Quais pocos estao com BSW acima de 80%?",
    "Quantos pocos estao produzindo por bacia?",
    "Mostre os 5 maiores custos de manutencao",
]

tab_genie = dbc.Container(
    fluid=True,
    className="py-3",
    children=[
        dbc.Row(
            [
                # Chat area
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5(
                                        [html.I(className="fas fa-robot me-2"), "Genie - Assistente de Dados"],
                                        className="text-info mb-3",
                                    ),
                                    # Chat history
                                    html.Div(
                                        id="genie-chat-history",
                                        style={
                                            "height": "55vh",
                                            "overflowY": "auto",
                                            "padding": "15px",
                                            "backgroundColor": "#0f0f23",
                                            "borderRadius": "8px",
                                            "border": "1px solid #16213e",
                                            "marginBottom": "15px",
                                        },
                                        children=[
                                            html.Div(
                                                [
                                                    html.I(className="fas fa-robot me-2", style={"color": "#00d4ff"}),
                                                    html.Span(
                                                        "Ola! Sou o assistente de dados da operadora. "
                                                        "Faca perguntas sobre producao, pocos e manutencao em linguagem natural.",
                                                        style={"color": "#ccd6f6"},
                                                    ),
                                                ],
                                                style={
                                                    "backgroundColor": "#16213e",
                                                    "padding": "12px 16px",
                                                    "borderRadius": "8px",
                                                    "marginBottom": "10px",
                                                },
                                            )
                                        ],
                                    ),
                                    # Input area
                                    dbc.InputGroup(
                                        [
                                            dbc.Input(
                                                id="genie-input",
                                                placeholder="Digite sua pergunta sobre producao, pocos ou manutencao...",
                                                type="text",
                                                className="bg-dark text-light",
                                                style={"borderColor": "#16213e"},
                                                n_submit=0,
                                            ),
                                            dbc.Button(
                                                [html.I(className="fas fa-paper-plane")],
                                                id="genie-send-btn",
                                                color="info",
                                                n_clicks=0,
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            style=CARD_STYLE,
                        ),
                    ],
                    width=8,
                ),
                # Sidebar
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6(
                                        [html.I(className="fas fa-lightbulb me-2"), "Perguntas Sugeridas"],
                                        className="text-info mb-3",
                                    ),
                                    *[
                                        dbc.Button(
                                            q,
                                            id=f"genie-sample-{i}",
                                            color="outline-secondary",
                                            size="sm",
                                            className="w-100 mb-2 text-start",
                                            style={"fontSize": "12px", "whiteSpace": "normal", "textAlign": "left"},
                                            n_clicks=0,
                                        )
                                        for i, q in enumerate(SAMPLE_QUESTIONS)
                                    ],
                                ]
                            ),
                            style=CARD_STYLE,
                        ),
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6(
                                        [html.I(className="fas fa-code me-2"), "SQL Gerado"],
                                        className="text-info mb-3",
                                    ),
                                    html.Pre(
                                        id="genie-sql-display",
                                        style={
                                            "backgroundColor": "#0f0f23",
                                            "color": "#00d4ff",
                                            "padding": "12px",
                                            "borderRadius": "8px",
                                            "fontSize": "11px",
                                            "maxHeight": "200px",
                                            "overflowY": "auto",
                                            "whiteSpace": "pre-wrap",
                                            "border": "1px solid #16213e",
                                        },
                                        children="-- Nenhuma query executada ainda",
                                    ),
                                ]
                            ),
                            style={**CARD_STYLE, "marginTop": "15px"},
                        ),
                    ],
                    width=4,
                ),
            ]
        ),
        # Stores
        dcc.Store(id="genie-conversation-id", data=""),
        dcc.Store(id="genie-chat-store", data=[]),
    ],
)


# ---------------------------------------------------------------------------
# Layout principal
# ---------------------------------------------------------------------------

app.layout = html.Div(
    style={"backgroundColor": "#0a0a1a", "minHeight": "100vh"},
    children=[
        header,
        dcc.Tabs(
            id="main-tabs",
            value="tab-mapa",
            children=[
                dcc.Tab(
                    label="  Mapa Operacional",
                    value="tab-mapa",
                    style=TAB_STYLE,
                    selected_style=TAB_SELECTED_STYLE,
                ),
                dcc.Tab(
                    label="  Dashboard Producao",
                    value="tab-dashboard",
                    style=TAB_STYLE,
                    selected_style=TAB_SELECTED_STYLE,
                ),
                dcc.Tab(
                    label="  Modelo Preditivo",
                    value="tab-modelo",
                    style=TAB_STYLE,
                    selected_style=TAB_SELECTED_STYLE,
                ),
                dcc.Tab(
                    label="  Genie Assistant",
                    value="tab-genie",
                    style=TAB_STYLE,
                    selected_style=TAB_SELECTED_STYLE,
                ),
            ],
            style={"backgroundColor": "#0f0f23"},
        ),
        html.Div(id="tab-content"),
        # Store para cache de dados de pocos
        dcc.Store(id="pocos-data-store"),
    ],
)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "value"),
)
def render_tab(tab):
    if tab == "tab-mapa":
        return tab_mapa
    elif tab == "tab-dashboard":
        return tab_dashboard
    elif tab == "tab-modelo":
        return tab_modelo
    elif tab == "tab-genie":
        return tab_genie
    return html.Div()


# -- Callback: Carregar dados de pocos quando tab mapa e selecionada --

@app.callback(
    Output("pocos-data-store", "data"),
    Input("main-tabs", "value"),
    prevent_initial_call=True,
)
def load_pocos_data(tab):
    if tab != "tab-mapa":
        return no_update
    df = load_pocos()
    if df.empty:
        return []
    return df.to_dict("records")


# -- Callback: Renderizar mapa de pocos --

@app.callback(
    [Output("mapa-pocos", "figure"), Output("mapa-stats", "children")],
    [Input("pocos-data-store", "data"), Input("mapa-campo-filter", "value")],
)
def update_mapa(data, campo_filter):
    # Figura vazia padrao
    empty_fig = go.Figure()
    empty_fig.update_layout(
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        font_color="#ccd6f6",
        mapbox=dict(style="open-street-map", center=dict(lat=-22.5, lon=-40.0), zoom=6),
        margin=dict(l=0, r=0, t=0, b=0),
    )

    if not data:
        empty_fig.add_annotation(
            text="Carregando dados dos pocos...<br>Verifique a conexao com o SQL Warehouse.",
            showarrow=False,
            font=dict(size=16, color="#8892b0"),
            xref="paper", yref="paper", x=0.5, y=0.5,
        )
        return empty_fig, html.P("Sem dados", className="text-muted")

    df = pd.DataFrame(data)

    # Filtrar por campo
    if campo_filter and campo_filter != "Todos":
        df = df[df["campo"] == campo_filter]

    if df.empty:
        return empty_fig, html.P("Nenhum poco encontrado", className="text-muted")

    # Mapear cores
    df["color"] = df["status"].map(STATUS_COLORS).fillna("#95a5a6")

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="status",
        color_discrete_map=STATUS_COLORS,
        hover_name="nome_poco",
        hover_data={
            "campo": True,
            "bacia": True,
            "tipo_poco": True,
            "metodo_elevacao": True,
            "profundidade_m": ":.0f",
            "status": True,
            "latitude": False,
            "longitude": False,
        },
        zoom=6,
        center={"lat": -22.5, "lon": -40.0},
        opacity=0.85,
        size_max=12,
    )

    fig.update_traces(marker=dict(size=9))

    fig.update_layout(
        mapbox_style="open-street-map",
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        font_color="#ccd6f6",
        legend=dict(
            title="Status",
            bgcolor="rgba(26,26,46,0.9)",
            bordercolor="#16213e",
            font=dict(color="#ccd6f6"),
        ),
        margin=dict(l=0, r=0, t=0, b=0),
    )

    # Stats
    total = len(df)
    produzindo = len(df[df["status"] == "Produzindo"])
    parado = len(df[df["status"] == "Parado"])
    manutencao = len(df[df["status"] == "Manutencao"])

    stats = html.Div(
        [
            html.P(
                [html.Strong(f"{total}", style={"color": "#00d4ff"}), " pocos no mapa"],
                className="text-light mb-1",
                style={"fontSize": "13px"},
            ),
            html.P(
                [html.Span("\u25CF ", style={"color": "#2ecc71"}), f"{produzindo} produzindo"],
                className="text-light mb-1",
                style={"fontSize": "12px"},
            ),
            html.P(
                [html.Span("\u25CF ", style={"color": "#e74c3c"}), f"{parado} parados"],
                className="text-light mb-1",
                style={"fontSize": "12px"},
            ),
            html.P(
                [html.Span("\u25CF ", style={"color": "#f1c40f"}), f"{manutencao} em manutencao"],
                className="text-light mb-0",
                style={"fontSize": "12px"},
            ),
        ]
    )

    return fig, stats


# -- Callback: Modelo Preditivo --

@app.callback(
    Output("modelo-resultado", "children"),
    Input("modelo-predict-btn", "n_clicks"),
    [
        State("modelo-campo", "value"),
        State("modelo-semana", "value"),
        State("modelo-bsw", "value"),
        State("modelo-pressao", "value"),
        State("modelo-temp", "value"),
        State("modelo-horas", "value"),
        State("modelo-pocos", "value"),
        State("modelo-vol-ant", "value"),
        State("modelo-vol-media", "value"),
    ],
    prevent_initial_call=True,
)
def predict_production(n_clicks, campo, semana, bsw, pressao, temp, horas, pocos, vol_ant, vol_media):
    if n_clicks == 0:
        return no_update

    if not SERVING_ENDPOINT:
        return dbc.Alert(
            [
                html.I(className="fas fa-exclamation-triangle me-2"),
                "Model Serving endpoint nao configurado. ",
                "Configure a variavel SERVING_ENDPOINT_NAME.",
            ],
            color="warning",
        )

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

    try:
        prediction = call_serving_endpoint(features)

        # Gauge chart
        max_val = max(prediction * 2, 200000)
        fig_gauge = go.Figure(
            go.Indicator(
                mode="gauge+number+delta",
                value=prediction,
                number={"suffix": " bbl", "font": {"size": 36, "color": "#00d4ff"}},
                delta={"reference": vol_ant, "relative": False, "suffix": " bbl"},
                title={"text": f"Producao Prevista - {campo}", "font": {"size": 18, "color": "#ccd6f6"}},
                gauge={
                    "axis": {"range": [0, max_val], "tickcolor": "#8892b0"},
                    "bar": {"color": "#00d4ff"},
                    "bgcolor": "#16213e",
                    "borderwidth": 0,
                    "steps": [
                        {"range": [0, max_val * 0.3], "color": "#e74c3c"},
                        {"range": [max_val * 0.3, max_val * 0.7], "color": "#f1c40f"},
                        {"range": [max_val * 0.7, max_val], "color": "#2ecc71"},
                    ],
                    "threshold": {
                        "line": {"color": "#ffffff", "width": 3},
                        "thickness": 0.8,
                        "value": vol_ant,
                    },
                },
            )
        )
        fig_gauge.update_layout(
            paper_bgcolor="#1a1a2e",
            plot_bgcolor="#1a1a2e",
            font_color="#ccd6f6",
            height=350,
            margin=dict(l=30, r=30, t=60, b=30),
        )

        return html.Div(
            [
                dcc.Graph(figure=fig_gauge, config={"displayModeBar": False}),
                html.Hr(style={"borderColor": "#16213e"}),
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H6("Previsao", className="text-muted mb-1", style={"fontSize": "11px"}),
                                        html.H4(
                                            f"{prediction:,.0f} bbl",
                                            className="text-info mb-0",
                                        ),
                                    ],
                                    style={"padding": "12px"},
                                ),
                                style={**CARD_STYLE, "backgroundColor": "#0f0f23"},
                            ),
                            width=4,
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H6("Sem. Anterior", className="text-muted mb-1", style={"fontSize": "11px"}),
                                        html.H4(
                                            f"{vol_ant:,.0f} bbl",
                                            className="text-light mb-0",
                                        ),
                                    ],
                                    style={"padding": "12px"},
                                ),
                                style={**CARD_STYLE, "backgroundColor": "#0f0f23"},
                            ),
                            width=4,
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H6("Variacao", className="text-muted mb-1", style={"fontSize": "11px"}),
                                        html.H4(
                                            f"{((prediction - vol_ant) / vol_ant * 100) if vol_ant else 0:+.1f}%",
                                            className="mb-0",
                                            style={
                                                "color": "#2ecc71" if prediction >= vol_ant else "#e74c3c"
                                            },
                                        ),
                                    ],
                                    style={"padding": "12px"},
                                ),
                                style={**CARD_STYLE, "backgroundColor": "#0f0f23"},
                            ),
                            width=4,
                        ),
                    ]
                ),
            ]
        )

    except Exception as e:
        logger.error(f"Erro na previsao: {e}")
        return dbc.Alert(
            [
                html.I(className="fas fa-exclamation-circle me-2"),
                f"Erro ao chamar o modelo: {str(e)}",
            ],
            color="danger",
        )


# -- Callback: Genie chat --

@app.callback(
    [
        Output("genie-chat-history", "children"),
        Output("genie-sql-display", "children"),
        Output("genie-conversation-id", "data"),
        Output("genie-chat-store", "data"),
        Output("genie-input", "value"),
    ],
    [
        Input("genie-send-btn", "n_clicks"),
        Input("genie-input", "n_submit"),
        *[Input(f"genie-sample-{i}", "n_clicks") for i in range(len(SAMPLE_QUESTIONS))],
    ],
    [
        State("genie-input", "value"),
        State("genie-conversation-id", "data"),
        State("genie-chat-store", "data"),
    ],
    prevent_initial_call=True,
)
def handle_genie_chat(send_clicks, n_submit, *args):
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update, no_update, no_update, no_update

    # Extrair states (ultimos 3 argumentos)
    sample_clicks = args[:-3]
    user_input = args[-3]
    conversation_id = args[-2]
    chat_history = args[-1] or []

    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]

    # Determinar a pergunta
    question = ""
    if triggered_id in ("genie-send-btn", "genie-input"):
        question = (user_input or "").strip()
    else:
        # Sample question clicked
        for i, q in enumerate(SAMPLE_QUESTIONS):
            if triggered_id == f"genie-sample-{i}":
                question = q
                break

    if not question:
        return no_update, no_update, no_update, no_update, no_update

    if not GENIE_SPACE_ID:
        error_msg = {
            "role": "assistant",
            "text": "Genie Space ID nao configurado. Configure a variavel GENIE_SPACE_ID.",
            "sql": "",
        }
        chat_history.append({"role": "user", "text": question, "sql": ""})
        chat_history.append(error_msg)
        chat_elements = _build_chat_elements(chat_history)
        return chat_elements, "-- Genie nao configurado", "", chat_history, ""

    # Adicionar mensagem do usuario
    chat_history.append({"role": "user", "text": question, "sql": ""})

    # Chamar Genie
    try:
        if conversation_id:
            result = genie_follow_up(conversation_id, question)
        else:
            result = genie_start_conversation(question)

        new_conv_id = result.get("conversation_id", conversation_id)

        if "error" in result:
            response_text = f"Erro: {result['error']}"
            sql_text = ""
        else:
            response_text = result.get("text", "Resposta recebida sem texto.").strip()
            sql_text = result.get("sql", "")

        chat_history.append({
            "role": "assistant",
            "text": response_text,
            "sql": sql_text,
        })

        chat_elements = _build_chat_elements(chat_history)
        sql_display = sql_text if sql_text else "-- Nenhuma query SQL gerada nesta resposta"

        return chat_elements, sql_display, new_conv_id, chat_history, ""

    except Exception as e:
        logger.error(f"Erro no Genie: {e}")
        chat_history.append({
            "role": "assistant",
            "text": f"Erro ao processar pergunta: {str(e)}",
            "sql": "",
        })
        chat_elements = _build_chat_elements(chat_history)
        return chat_elements, f"-- Erro: {str(e)}", conversation_id, chat_history, ""


def _build_chat_elements(chat_history: list) -> list:
    """Constroi elementos HTML do historico de chat."""
    elements = [
        html.Div(
            [
                html.I(className="fas fa-robot me-2", style={"color": "#00d4ff"}),
                html.Span(
                    "Ola! Sou o assistente de dados da operadora. "
                    "Faca perguntas sobre producao, pocos e manutencao em linguagem natural.",
                    style={"color": "#ccd6f6"},
                ),
            ],
            style={
                "backgroundColor": "#16213e",
                "padding": "12px 16px",
                "borderRadius": "8px",
                "marginBottom": "10px",
            },
        )
    ]

    for msg in chat_history:
        if msg["role"] == "user":
            elements.append(
                html.Div(
                    [
                        html.I(className="fas fa-user me-2", style={"color": "#2ecc71"}),
                        html.Span(msg["text"], style={"color": "#ccd6f6"}),
                    ],
                    style={
                        "backgroundColor": "#1e3a5f",
                        "padding": "12px 16px",
                        "borderRadius": "8px",
                        "marginBottom": "10px",
                        "marginLeft": "40px",
                    },
                )
            )
        else:
            children = [
                html.I(className="fas fa-robot me-2", style={"color": "#00d4ff"}),
                html.Span(msg["text"], style={"color": "#ccd6f6"}),
            ]
            # Mostrar SQL inline se existir
            if msg.get("sql"):
                children.append(
                    html.Details(
                        [
                            html.Summary(
                                "Ver SQL gerado",
                                style={"color": "#00d4ff", "cursor": "pointer", "fontSize": "12px", "marginTop": "8px"},
                            ),
                            html.Pre(
                                msg["sql"],
                                style={
                                    "backgroundColor": "#0a0a1a",
                                    "color": "#00d4ff",
                                    "padding": "8px",
                                    "borderRadius": "4px",
                                    "fontSize": "11px",
                                    "marginTop": "4px",
                                    "whiteSpace": "pre-wrap",
                                },
                            ),
                        ]
                    )
                )

            elements.append(
                html.Div(
                    children,
                    style={
                        "backgroundColor": "#16213e",
                        "padding": "12px 16px",
                        "borderRadius": "8px",
                        "marginBottom": "10px",
                        "marginRight": "40px",
                    },
                )
            )

    return elements


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", 8000)))
    logger.info(f"Iniciando O&G Operations Hub na porta {port}")
    logger.info(f"Catalogo: {CATALOGO} | Schema: {SCHEMA}")
    logger.info(f"Warehouse ID: {'configurado' if WAREHOUSE_ID else 'NAO CONFIGURADO'}")
    logger.info(f"Serving Endpoint: {SERVING_ENDPOINT or 'NAO CONFIGURADO'}")
    logger.info(f"Genie Space ID: {GENIE_SPACE_ID or 'NAO CONFIGURADO'}")
    app.run(host="0.0.0.0", port=port, debug=False)
