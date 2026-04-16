"""
O&G Operations Hub - Databricks App
====================================
Aplicacao Dash para monitoramento de operacoes de Oil & Gas.
3 abas: Dashboard Producao, Modelo Preditivo, Genie Assistant.
"""

import os
import json
import time
import logging
import requests
import pandas as pd
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
    "background": "rgba(26, 26, 46, 0.85)",
    "border": "1px solid rgba(0, 212, 255, 0.15)",
    "borderRadius": "14px",
    "backdropFilter": "blur(10px)",
    "WebkitBackdropFilter": "blur(10px)",
    "boxShadow": "0 4px 24px rgba(0, 0, 0, 0.4)",
}

TAB_STYLE = {
    "backgroundColor": "transparent",
    "borderColor": "transparent",
    "color": "#8892b0",
    "padding": "14px 28px",
    "fontWeight": "500",
    "fontSize": "14px",
    "letterSpacing": "0.3px",
    "borderRadius": "8px 8px 0 0",
}

TAB_SELECTED_STYLE = {
    "backgroundColor": "rgba(0, 212, 255, 0.08)",
    "borderColor": "transparent",
    "borderTop": "3px solid #00d4ff",
    "color": "#00d4ff",
    "padding": "14px 28px",
    "fontWeight": "700",
    "fontSize": "14px",
    "letterSpacing": "0.3px",
    "borderRadius": "8px 8px 0 0",
}

HEADER_STYLE = {
    "background": "linear-gradient(135deg, #0a0a1a 0%, #12122a 40%, #1a1040 100%)",
    "padding": "22px 36px",
    "borderBottom": "1px solid rgba(0, 212, 255, 0.25)",
    "boxShadow": "0 2px 20px rgba(0, 212, 255, 0.08)",
    "marginBottom": "0px",
}

ACCENT_CYAN = "#00d4ff"
ACCENT_PURPLE = "#7c3aed"

GRADIENT_CARD_STYLE = {
    "background": "linear-gradient(135deg, rgba(0,212,255,0.08) 0%, rgba(124,58,237,0.08) 100%)",
    "border": "1px solid rgba(0, 212, 255, 0.2)",
    "borderRadius": "14px",
    "backdropFilter": "blur(10px)",
    "WebkitBackdropFilter": "blur(10px)",
    "boxShadow": "0 4px 24px rgba(0, 0, 0, 0.4)",
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
                        html.Div(
                            [
                                html.Span(
                                    html.I(className="fas fa-oil-well"),
                                    style={
                                        "background": "linear-gradient(135deg, #00d4ff, #7c3aed)",
                                        "WebkitBackgroundClip": "text",
                                        "WebkitTextFillColor": "transparent",
                                        "fontSize": "28px",
                                        "marginRight": "12px",
                                        "display": "inline-block",
                                        "verticalAlign": "middle",
                                    },
                                ),
                                html.Span(
                                    "O&G Operations Hub",
                                    style={
                                        "background": "linear-gradient(90deg, #e6f1ff 0%, #00d4ff 60%, #7c3aed 100%)",
                                        "WebkitBackgroundClip": "text",
                                        "WebkitTextFillColor": "transparent",
                                        "fontSize": "26px",
                                        "fontWeight": "800",
                                        "letterSpacing": "-0.5px",
                                        "verticalAlign": "middle",
                                    },
                                ),
                            ],
                            style={"marginBottom": "6px"},
                        ),
                        html.P(
                            [
                                html.Span("Plataforma Integrada de Monitoramento", style={"color": "#8892b0"}),
                                html.Span(" · ", style={"color": "#334", "margin": "0 6px"}),
                                html.Span(
                                    [
                                        html.I(className="fas fa-bolt me-1", style={"fontSize": "10px"}),
                                        "Powered by Databricks",
                                    ],
                                    style={
                                        "color": "#00d4ff",
                                        "backgroundColor": "rgba(0,212,255,0.08)",
                                        "padding": "2px 8px",
                                        "borderRadius": "4px",
                                        "fontSize": "12px",
                                        "fontWeight": "600",
                                    },
                                ),
                            ],
                            style={"marginBottom": "0", "fontSize": "13px"},
                        ),
                    ],
                    width=8,
                ),
                dbc.Col(
                    html.Div(
                        [
                            dbc.Badge(
                                [html.I(className="fas fa-database me-1"), "SQL Warehouse"],
                                color="success" if WAREHOUSE_ID else "danger",
                                className="me-2 badge-pulse",
                                pill=True,
                                style={"fontSize": "11px", "padding": "6px 12px"},
                            ),
                            dbc.Badge(
                                [html.I(className="fas fa-brain me-1"), "Model Serving"],
                                color="success" if SERVING_ENDPOINT else "danger",
                                className="me-2 badge-pulse-purple",
                                pill=True,
                                style={"fontSize": "11px", "padding": "6px 12px"},
                            ),
                            dbc.Badge(
                                [html.I(className="fas fa-robot me-1"), "Genie"],
                                color="success" if GENIE_SPACE_ID else "danger",
                                className="badge-pulse",
                                pill=True,
                                style={"fontSize": "11px", "padding": "6px 12px"},
                            ),
                        ],
                        style={"textAlign": "right", "paddingTop": "10px"},
                    ),
                    width=4,
                ),
            ],
            align="center",
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
                            html.Div(
                                [
                                    html.Div(
                                        html.I(
                                            className="fas fa-chart-area",
                                            style={"fontSize": "48px", "color": "#00d4ff"},
                                        ),
                                        style={
                                            "background": "linear-gradient(135deg, rgba(0,212,255,0.15), rgba(124,58,237,0.15))",
                                            "borderRadius": "50%",
                                            "width": "96px",
                                            "height": "96px",
                                            "display": "flex",
                                            "alignItems": "center",
                                            "justifyContent": "center",
                                            "margin": "0 auto 24px",
                                            "border": "1px solid rgba(0,212,255,0.3)",
                                        },
                                    ),
                                    html.H4(
                                        "Dashboard AI/BI",
                                        style={"color": "#e6f1ff", "fontWeight": "700", "marginBottom": "6px"},
                                    ),
                                    html.P(
                                        "Embed do Painel Executivo de Producao",
                                        className="text-muted mb-4",
                                        style={"fontSize": "14px"},
                                    ),
                                    dbc.Alert(
                                        [
                                            html.I(className="fas fa-info-circle me-2"),
                                            "Configure a variavel de ambiente ",
                                            html.Code(
                                                "DASHBOARD_EMBED_URL",
                                                style={
                                                    "color": "#00d4ff",
                                                    "backgroundColor": "rgba(0,212,255,0.1)",
                                                    "padding": "2px 6px",
                                                    "borderRadius": "4px",
                                                },
                                            ),
                                            " com a URL de embed do dashboard publicado.",
                                        ],
                                        color="info",
                                        style={
                                            "backgroundColor": "rgba(0,212,255,0.07)",
                                            "borderColor": "rgba(0,212,255,0.25)",
                                            "color": "#ccd6f6",
                                            "fontSize": "13px",
                                        },
                                    ),
                                    html.Hr(style={"borderColor": "rgba(0,212,255,0.1)", "width": "60%", "margin": "20px auto"}),
                                    html.Div(
                                        [
                                            html.Span(
                                                [html.I(className="fas fa-share-alt me-1", style={"color": "#7c3aed"}), "Dashboard"],
                                                style={"color": "#ccd6f6", "fontSize": "13px"},
                                            ),
                                            html.Span(" → ", style={"color": "#8892b0", "margin": "0 10px"}),
                                            html.Span(
                                                [html.I(className="fas fa-share me-1", style={"color": "#7c3aed"}), "Share"],
                                                style={"color": "#ccd6f6", "fontSize": "13px"},
                                            ),
                                            html.Span(" → ", style={"color": "#8892b0", "margin": "0 10px"}),
                                            html.Span(
                                                [html.I(className="fas fa-code me-1", style={"color": "#00d4ff"}), "Embed > Copy URL"],
                                                style={"color": "#00d4ff", "fontSize": "13px"},
                                            ),
                                        ],
                                        style={"display": "flex", "alignItems": "center", "justifyContent": "center", "flexWrap": "wrap", "gap": "4px"},
                                    ),
                                ],
                                style={"textAlign": "center", "padding": "56px 40px"},
                            )
                        ),
                        style=GRADIENT_CARD_STYLE,
                    ),
                    width=7,
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
                                        [html.I(className="fas fa-sliders-h me-2", style={"color": "#7c3aed"}), "Parametros de Entrada"],
                                        style={"color": "#e6f1ff", "fontWeight": "700", "marginBottom": "16px"},
                                    ),
                                    # Campo
                                    html.Label("Campo", className="text-light mb-1", style={"fontSize": "13px"}),
                                    dcc.Dropdown(
                                        id="modelo-campo",
                                        options=[{"label": c, "value": c} for c in CAMPOS],
                                        value="Frade",
                                        clearable=False,
                                        style={"backgroundColor": "#16213e", "color": "#e6f1ff"},
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
                                    html.Hr(style={"borderColor": "rgba(0,212,255,0.1)", "margin": "12px 0"}),
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
                                                        style={"borderColor": "rgba(0,212,255,0.2)"},
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
                                                        style={"borderColor": "rgba(0,212,255,0.2)"},
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
                                                        style={"borderColor": "rgba(0,212,255,0.2)"},
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
                                                        style={"borderColor": "rgba(0,212,255,0.2)"},
                                                    ),
                                                ],
                                                width=6,
                                            ),
                                        ],
                                        className="mb-3",
                                    ),
                                    html.Hr(style={"borderColor": "rgba(0,212,255,0.1)", "margin": "12px 0"}),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    html.Label("Pocos Ativos", className="text-light mb-1", style={"fontSize": "13px"}),
                                                    dbc.Input(
                                                        id="modelo-pocos", type="number", value=50,
                                                        min=1, max=500, step=1,
                                                        className="bg-dark text-light",
                                                        style={"borderColor": "rgba(0,212,255,0.2)"},
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
                                                        style={"borderColor": "rgba(0,212,255,0.2)"},
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
                                                        style={"borderColor": "rgba(0,212,255,0.2)"},
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
                                        style={
                                            "background": "linear-gradient(135deg, #00d4ff, #7c3aed)",
                                            "border": "none",
                                            "fontWeight": "700",
                                            "letterSpacing": "0.5px",
                                            "boxShadow": "0 4px 15px rgba(0,212,255,0.25)",
                                        },
                                    ),
                                ]
                            ),
                            style=GRADIENT_CARD_STYLE,
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
                                        [html.I(className="fas fa-chart-bar me-2", style={"color": "#00d4ff"}), "Resultado da Previsao"],
                                        style={"color": "#e6f1ff", "fontWeight": "700", "marginBottom": "16px"},
                                    ),
                                    dcc.Loading(
                                        html.Div(id="modelo-resultado"),
                                        type="circle",
                                        color="#00d4ff",
                                    ),
                                ]
                            ),
                            style=GRADIENT_CARD_STYLE,
                        ),
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6(
                                        [html.I(className="fas fa-info-circle me-2", style={"color": "#7c3aed"}), "Sobre o Modelo"],
                                        style={"color": "#ccd6f6", "marginBottom": "12px"},
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
                            style={**CARD_STYLE, "marginTop": "15px", "border": "1px solid rgba(124,58,237,0.2)"},
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
                                        [html.I(className="fas fa-robot me-2", style={"color": "#7c3aed"}), "Genie — Assistente de Dados"],
                                        style={"color": "#e6f1ff", "fontWeight": "700", "marginBottom": "14px"},
                                    ),
                                    # Chat history
                                    html.Div(
                                        id="genie-chat-history",
                                        style={
                                            "height": "55vh",
                                            "overflowY": "auto",
                                            "padding": "15px",
                                            "background": "rgba(8,8,24,0.7)",
                                            "borderRadius": "10px",
                                            "border": "1px solid rgba(0,212,255,0.1)",
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
                                                    "background": "linear-gradient(135deg, rgba(124,58,237,0.18), rgba(0,212,255,0.08))",
                                                    "border": "1px solid rgba(124,58,237,0.25)",
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
                                                style={"borderColor": "rgba(0,212,255,0.3)", "backgroundColor": "rgba(8,8,24,0.8)", "color": "#e6f1ff"},
                                                n_submit=0,
                                            ),
                                            dbc.Button(
                                                [html.I(className="fas fa-paper-plane")],
                                                id="genie-send-btn",
                                                color="info",
                                                n_clicks=0,
                                                style={"background": "linear-gradient(135deg, #00d4ff, #7c3aed)", "border": "none"},
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            style=GRADIENT_CARD_STYLE,
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
                                        [html.I(className="fas fa-lightbulb me-2", style={"color": "#7c3aed"}), "Perguntas Sugeridas"],
                                        style={"color": "#ccd6f6", "marginBottom": "12px"},
                                    ),
                                    *[
                                        dbc.Button(
                                            q,
                                            id=f"genie-sample-{i}",
                                            color="outline-info",
                                            size="sm",
                                            className="w-100 mb-2 text-start",
                                            style={
                                                "fontSize": "12px",
                                                "whiteSpace": "normal",
                                                "textAlign": "left",
                                                "borderColor": "rgba(0,212,255,0.2)",
                                                "color": "#8892b0",
                                            },
                                            n_clicks=0,
                                        )
                                        for i, q in enumerate(SAMPLE_QUESTIONS)
                                    ],
                                ]
                            ),
                            style=GRADIENT_CARD_STYLE,
                        ),
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6(
                                        [html.I(className="fas fa-code me-2", style={"color": "#00d4ff"}), "SQL Gerado"],
                                        style={"color": "#ccd6f6", "marginBottom": "12px"},
                                    ),
                                    html.Pre(
                                        id="genie-sql-display",
                                        style={
                                            "background": "rgba(8,8,24,0.8)",
                                            "color": "#00d4ff",
                                            "padding": "12px",
                                            "borderRadius": "8px",
                                            "fontSize": "11px",
                                            "maxHeight": "200px",
                                            "overflowY": "auto",
                                            "whiteSpace": "pre-wrap",
                                            "border": "1px solid rgba(124,58,237,0.2)",
                                        },
                                        children="-- Nenhuma query executada ainda",
                                    ),
                                ]
                            ),
                            style={**CARD_STYLE, "marginTop": "15px", "border": "1px solid rgba(124,58,237,0.25)"},
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
    style={"backgroundColor": "#080818", "minHeight": "100vh"},
    children=[
        header,
        dcc.Tabs(
            id="main-tabs",
            value="tab-dashboard",
            children=[
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
            style={"backgroundColor": "#0a0a1a", "borderBottom": "1px solid rgba(0,212,255,0.1)", "paddingLeft": "12px"},
        ),
        html.Div(id="tab-content"),
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
    if tab == "tab-dashboard":
        return tab_dashboard
    elif tab == "tab-modelo":
        return tab_modelo
    elif tab == "tab-genie":
        return tab_genie
    return tab_dashboard


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
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
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
                                            style={"color": "#00d4ff", "marginBottom": "0"},
                                        ),
                                    ],
                                    style={"padding": "12px", "textAlign": "center"},
                                ),
                                style={
                                    **CARD_STYLE,
                                    "background": "linear-gradient(135deg, rgba(0,212,255,0.15), rgba(0,212,255,0.05))",
                                    "border": "1px solid rgba(0,212,255,0.3)",
                                },
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
                                    style={"padding": "12px", "textAlign": "center"},
                                ),
                                style={**CARD_STYLE},
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
                                    style={"padding": "12px", "textAlign": "center"},
                                ),
                                style={
                                    **CARD_STYLE,
                                    "background": "linear-gradient(135deg, rgba(124,58,237,0.15), rgba(124,58,237,0.05))",
                                    "border": "1px solid rgba(124,58,237,0.3)",
                                },
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
                "background": "linear-gradient(135deg, rgba(124,58,237,0.18), rgba(0,212,255,0.08))",
                "border": "1px solid rgba(124,58,237,0.25)",
                "padding": "12px 16px",
                "borderRadius": "10px",
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
                        "background": "linear-gradient(135deg, rgba(0,212,255,0.12), rgba(0,100,180,0.08))",
                        "border": "1px solid rgba(0,212,255,0.2)",
                        "padding": "12px 16px",
                        "borderRadius": "10px",
                        "marginBottom": "10px",
                        "marginLeft": "40px",
                    },
                )
            )
        else:
            children = [
                html.I(className="fas fa-robot me-2", style={"color": "#7c3aed"}),
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
                                    "background": "rgba(8,8,24,0.9)",
                                    "color": "#00d4ff",
                                    "padding": "8px",
                                    "borderRadius": "4px",
                                    "fontSize": "11px",
                                    "marginTop": "4px",
                                    "whiteSpace": "pre-wrap",
                                    "border": "1px solid rgba(124,58,237,0.2)",
                                },
                            ),
                        ]
                    )
                )

            elements.append(
                html.Div(
                    children,
                    style={
                        "background": "linear-gradient(135deg, rgba(124,58,237,0.12), rgba(26,26,46,0.85))",
                        "border": "1px solid rgba(124,58,237,0.2)",
                        "padding": "12px 16px",
                        "borderRadius": "10px",
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
