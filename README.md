<h1 align="center">⛽ Workshop Hands-On Databricks — Oil & Gas</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks">
  <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake">
  <img src="https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white" alt="MLflow">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white" alt="SQL">
</p>

<p align="center">
  Workshop pratico de <strong>Data Engineering, Machine Learning e Analytics</strong> aplicados ao setor de <strong>Petroleo & Gas</strong>, utilizando a plataforma Databricks.
</p>


## 📋 Agenda

| # | Atividade | Duracao | Descricao |
|---|-----------|---------|-----------|
| ⚙️ | **Setup Inicial** | 15 min | Configuracao do catalogo + geracao de dados sinteticos O&G |
| 1️⃣ | **Lab 1 — Spark Declarative Pipelines** | 40 min | Auto Loader, Pipeline Medallion (Bronze/Silver/Gold), DQ, Streaming SCADA |
| 2️⃣ | **Lab 2 — Machine Learning** | 35 min | Previsao de declinio, Champion/Challenger, MLflow, Inferencia batch |
| 3️⃣ | **Lab 3 — AI/BI** | 25 min | Genie Space (linguagem natural) + Dashboard (importar JSON) |
| 4️⃣ | **Lab 4 — Databricks App** | 20 min | Demo do O&G Operations Hub (Mapa, Dashboard, ML Serving, Genie Chat) |
| | **Total** | **~2h00** | |
---

## 🏗️ Arquitetura

<!--
<p align="center">
  <img src="images/arquitetura.png" alt="Arquitetura Medallion — Oil & Gas" width="90%">
</p>
-->

O workshop segue a arquitetura **Medallion (Bronze → Silver → Gold)**, processando dados de operacoes de petroleo offshore desde a ingestao via SAP HANA e SCADA ate dashboards, ML e apps:

- **Fontes**: CSVs do SAP (PM, PP, FI/CO) + JSONs de streaming SCADA
- **Bronze**: Dados brutos com metadados de ingestao (Auto Loader)
- **Silver**: Dados limpos com Data Quality Expectations
- **Gold**: KPIs agregados (producao por campo, custos de manutencao, eficiencia)
- **Consumo**: Dashboard AI/BI, Genie, MLflow + Model Serving, Databricks App

---

## 📊 Modelo de Dados

### Volumes (Landing Zone)

| Volume | Formato | Registros | Simula |
|--------|---------|-----------|--------|
| `pocos_csv` | CSV | ~2.000 | SAP PM — cadastro de pocos offshore |
| `producao_csv` | CSV | ~15.000 | SAP PP — producao historica |
| `ordens_json` | JSON | ~5.000 | SAP PM + FI/CO — ordens de manutencao |
| `producao_streaming` | JSON | *(vazio)* | SCADA — populado pelo gerador no Lab 01 |

### Tabelas criadas pelo Pipeline

| Camada | Tabelas | Descricao |
|--------|---------|-----------|
| **Bronze** | `bronze_pocos`, `bronze_producao`, `bronze_ordens` | Dados brutos + metadados de auditoria |
| **Silver** | `silver_pocos`, `silver_producao`, `silver_ordens` | Dados validados, tipados e padronizados |
| **Gold** | `gold_producao_por_campo`, `gold_custos_manutencao`, `gold_eficiencia_operacional` | KPIs prontos para dashboards e ML |

> **Campos representados:** Frade, Tubarao Martelo, Polvo, Albacora Leste, Wahoo, Peregrino, Marlim, Roncador, Jubarte, Buzios (Bacias de Campos, Santos e Espirito Santo)

---

## 📁 Estrutura do Repositorio

```
workshop_databricks/
├── 📄 README.md
├── 📄 guia_apresentador.txt               ← Jargoes O&G, decisoes tecnicas
│
├── 📂 00_Setup/
│   ├── 00_configuracao_catalogo.py        ← Isolamento por participante (current_user)
│   └── 01_dados_sinteticos.py             ← Dados O&G → Volumes (CSV/JSON)
│
├── 📂 01_Lab_SDP/
│   ├── 01a_gerador_streaming.py           ← Gerador SCADA em tempo real
│   ├── 01d_validacao_pipeline.py          ← Contagem + versoes Delta Lake
│   ├── 📂 sql/
│   │   ├── 01b_sdp_pipeline_to_do.sql     ← Pipeline SQL (5 TO-DOs)
│   │   └── 01c_sdp_pipeline_completo.sql
│   └── 📂 python/
│       ├── 01b_sdp_pipeline_to_do.py      ← Pipeline Python (5 TO-DOs)
│       └── 01c_sdp_pipeline_completo.py
│
├── 📂 02_Lab_ML/
│   ├── 02a_ml_to_do.py                    ← Exercicios ML (3 TO-DOs + extras)
│   └── 02b_ml_completo.py                 ← Solucao de referencia
│
├── 📂 03_Lab_AIBI/
│   ├── 03a_aibi_genie.py                  ← Criar Genie Space
│   ├── 03b_aibi_dashboard.py              ← Importar dashboard JSON
│   └── *.lvdash.json                      ← Dashboard pre-configurado
│
├── 📂 04_App/
│   ├── app.py                             ← Dash app (4 tabs)
│   ├── app.yaml                           ← Config de recursos
│   ├── requirements.txt
│   └── 04_deploy_app.py                   ← Instrucoes de deploy
│
└── 📂 99_Cleanup/
    └── 99_cleanup.py                      ← DROP SCHEMA CASCADE
```

---

## 🔧 Pre-requisitos

### Workspace Databricks
- ✅ Unity Catalog habilitado
- ✅ DBR 14.0+ ou Serverless Compute
- ✅ SQL Warehouse (Labs 01, 03 e 04)

### Permissoes por Lab

| Lab | Permissoes |
|-----|-----------|
| ⚙️ Setup | `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME` |
| 1️⃣ SDP | `CREATE TABLE`, gerenciar pipelines (modo Continuous) |
| 2️⃣ ML | `CREATE EXPERIMENT`, registrar modelos no UC, Model Serving |
| 3️⃣ AI/BI | Criar Genie Space, importar dashboard |
| 4️⃣ App | Criar Databricks App, adicionar recursos |

---

## 🚀 Como executar

1. ⚙️ Importe a pasta para o Workspace Databricks
2. ⚙️ Execute `00_Setup/00_configuracao_catalogo.py` (schema via `current_user()`)
3. ⚙️ Execute `00_Setup/01_dados_sinteticos.py` (dados → Volumes)
4. 1️⃣ Crie o pipeline (SQL ou Python), configure modo **Continuous**, execute
5. 1️⃣ Inicie `01a_gerador_streaming` em outra sessao, valide com `01d`
6. 2️⃣ Feature engineering, treinar modelo, Champion vs Challenger, inferencia
7. 3️⃣ Crie o Genie Space, importe o dashboard JSON
8. 4️⃣ Deploy do O&G Operations Hub (demo)
9. 🧹 Execute `99_Cleanup/99_cleanup.py`

---

## 👤 Isolamento por Participante

| Aspecto | Como funciona |
|---------|--------------|
| **Catalogo** | `workshop_databricks` (compartilhado) |
| **Schema** | Derivado de `current_user()` → `gabriel_rangel` |
| **Tabelas** | Prefixo de camada: `bronze_`, `silver_`, `gold_` |
| **Pipeline** | Parametro `schema` na configuracao |
| **Experimento ML** | Path unico por usuario no Workspace |

---

## 🛠️ Tecnologias

| Categoria | Tecnologias |
|-----------|------------|
| **Ingestao** | Auto Loader (CSV + JSON), Lakeflow Connect (SAP HANA — conceito) |
| **Pipelines** | Spark Declarative Pipelines (SQL + Python), modo Continuous |
| **Data Quality** | Expectations (`expect`, `expect_or_drop`, `expect_or_fail`) |
| **Storage** | Delta Lake, Unity Catalog Volumes |
| **Governanca** | Unity Catalog (catalogo, schemas, lineage) |
| **ML** | scikit-learn, MLflow autolog, Model Registry (champion/challenger), Spark UDF |
| **Analytics** | AI/BI Dashboards, Genie Spaces (NLP em portugues) |
| **Apps** | Databricks Apps (Dash — mapa, dashboard embed, model serving, genie chat) |

---

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white" alt="Databricks">
  <br>
  <em>Workshop desenvolvido por Gabriel Rangel — Solutions Architect</em>
</p>
