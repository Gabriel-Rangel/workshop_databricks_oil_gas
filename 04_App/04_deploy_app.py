# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 04: Databricks App
# MAGIC ## Deploy do O&G Operations Hub
# MAGIC
# MAGIC **Objetivo:** Fazer o deploy da aplicacao web "O&G Operations Hub" como um
# MAGIC **Databricks App** — uma interface integrada para engenheiros de producao
# MAGIC acessarem mapas, dashboards, modelos e o assistente Genie.
# MAGIC
# MAGIC ### O que a aplicacao oferece:
# MAGIC
# MAGIC | Aba | Funcionalidade | Recurso Databricks |
# MAGIC |-----|---------------|--------------------|
# MAGIC | Mapa Operacional | Mapa interativo de pocos com filtros | SQL Warehouse |
# MAGIC | Dashboard Producao | Embed do dashboard AI/BI | Dashboard publicado |
# MAGIC | Modelo Preditivo | Previsao de producao via formulario | Model Serving |
# MAGIC | Genie Assistant | Chat com linguagem natural | Genie Space |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 0: Configuracao

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome

print(f"Ambiente: {CATALOGO}.{SCHEMA}")
print(f"Usuario: {usuario}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 1: Habilitar o Model Serving Endpoint
# MAGIC
# MAGIC O modelo `modelo_producao_decline@champion` precisa estar servido via
# MAGIC **Model Serving** para que a aba "Modelo Preditivo" funcione.
# MAGIC
# MAGIC 1. No menu lateral, va em **Serving**
# MAGIC 2. Clique em **Create serving endpoint**
# MAGIC 3. Configure:
# MAGIC    - **Name:** `workshop-modelo-{seu_nome}` (ex: `workshop-modelo-gabriel_rangel`)
# MAGIC    - **Entity:** `workshop_databricks.{seu_schema}.modelo_producao_decline` versao Champion
# MAGIC    - **Compute size:** Small
# MAGIC    - **Scale to zero:** Habilitado (para economizar)
# MAGIC 4. Clique em **Create**
# MAGIC 5. Aguarde o endpoint ficar no status **Ready** (pode levar 5-10 min)

# COMMAND ----------

# Validar que o modelo existe no Unity Catalog
model_name = f"{CATALOGO}.{SCHEMA}.modelo_producao_decline"
from mlflow import MlflowClient
import mlflow

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

try:
    champion = client.get_model_version_by_alias(model_name, "champion")
    print(f"Modelo Champion encontrado: {model_name}")
    print(f"  Versao: {champion.version}")
    print(f"  Status: {champion.status}")
    print(f"  Run ID: {champion.run_id}")
except Exception as e:
    print(f"ERRO: Modelo Champion nao encontrado! Execute o Lab 02 primeiro.")
    print(f"  Detalhes: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 2: Obter IDs necessarios
# MAGIC
# MAGIC Voce precisara dos seguintes IDs para configurar o app:

# COMMAND ----------

# Listar SQL Warehouses disponiveis
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("=== SQL Warehouses Disponiveis ===")
for wh in w.warehouses.list():
    state = wh.state.value if wh.state else "UNKNOWN"
    print(f"  [{state}] {wh.name} -> ID: {wh.id}")

# COMMAND ----------

# Listar Serving Endpoints
print("\n=== Model Serving Endpoints ===")
for ep in w.serving_endpoints.list():
    state = ep.state.ready if ep.state else "UNKNOWN"
    print(f"  [{state}] {ep.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 3: Anotar os IDs
# MAGIC
# MAGIC Anote os seguintes valores (voce precisara deles no Passo 5):
# MAGIC
# MAGIC | Recurso | Onde encontrar | Valor |
# MAGIC |---------|---------------|-------|
# MAGIC | **SQL Warehouse ID** | Listado acima | `_______________` |
# MAGIC | **Serving Endpoint Name** | Nome do endpoint criado no Passo 1 | `workshop-modelo-{nome}` |
# MAGIC | **Genie Space ID** | URL do Genie Space (Lab 03a) | `_______________` |
# MAGIC | **Dashboard Embed URL** | Dashboard > Share > Embed (Lab 03b) | `_______________` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 4: Fazer upload dos arquivos do app
# MAGIC
# MAGIC Os arquivos da aplicacao estao na pasta `04_App/`.
# MAGIC Precisamos fazer upload para o workspace.

# COMMAND ----------

import os

# Verificar se os arquivos do app existem
app_files = ["app.py", "app.yaml", "requirements.txt"]
workspace_path = f"/Workspace/Users/{usuario}/workshop_databricks/04_App"

print(f"Workspace path: {workspace_path}")
print(f"\nOs arquivos do app devem estar em: 04_App/")
print(f"Arquivos necessarios:")
for f in app_files:
    print(f"  - {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 5: Criar e configurar o Databricks App
# MAGIC
# MAGIC **Via UI:**
# MAGIC
# MAGIC 1. No menu lateral, clique em **Compute** > **Apps**
# MAGIC 2. Clique em **Create app**
# MAGIC 3. Configure:
# MAGIC    - **App name:** `og-hub-{seu_nome}` (ex: `og-hub-gabriel-rangel`)
# MAGIC    - **Description:** `O&G Operations Hub - Monitoramento de Operacoes`
# MAGIC
# MAGIC 4. Em **App resources**, adicione os seguintes recursos:
# MAGIC
# MAGIC    | Tipo | Key (na app.yaml) | Recurso | Permissao |
# MAGIC    |------|-------------------|---------|-----------|
# MAGIC    | SQL Warehouse | `sql-warehouse` | Selecione o warehouse | Can use |
# MAGIC    | Model Serving Endpoint | `serving-endpoint` | `workshop-modelo-{nome}` | Can query |
# MAGIC    | Genie Space | `genie-space` | Seu Genie Space do Lab 03 | Can run |
# MAGIC
# MAGIC 5. Em **Environment variables**, configure:
# MAGIC    - `SCHEMA` = `{seu_schema}` (ex: `gabriel_rangel`)
# MAGIC    - `DASHBOARD_EMBED_URL` = URL do embed do dashboard (opcional)
# MAGIC
# MAGIC 6. Clique em **Create**
# MAGIC
# MAGIC ### Passo 6: Deploy
# MAGIC
# MAGIC 1. Apos criar o app, clique em **Deploy**
# MAGIC 2. Em **Source code path**, selecione: `/Workspace/Users/{usuario}/workshop_databricks/04_App`
# MAGIC 3. Clique em **Deploy**
# MAGIC 4. Aguarde o deploy completar (observe os logs)
# MAGIC 5. Acesse a URL do app!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternativa: Deploy via CLI (avancado)
# MAGIC
# MAGIC Se preferir usar o terminal:
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Upload dos arquivos
# MAGIC databricks workspace mkdirs /Workspace/Users/<email>/workshop_databricks/04_App
# MAGIC databricks workspace import-dir ./04_App /Workspace/Users/<email>/workshop_databricks/04_App
# MAGIC
# MAGIC # 2. Criar o app
# MAGIC databricks apps create og-hub-<nome> --description "O&G Operations Hub"
# MAGIC
# MAGIC # 3. Deploy
# MAGIC databricks apps deploy og-hub-<nome> \
# MAGIC   --source-code-path /Workspace/Users/<email>/workshop_databricks/04_App
# MAGIC
# MAGIC # 4. Verificar status
# MAGIC databricks apps get og-hub-<nome>
# MAGIC
# MAGIC # 5. Ver logs
# MAGIC databricks apps logs og-hub-<nome>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 7: Validar o App
# MAGIC
# MAGIC Apos o deploy:
# MAGIC
# MAGIC | # | Teste | Resultado esperado |
# MAGIC |---|-------|-------------------|
# MAGIC | 1 | Abra a URL do app | Pagina carrega com tema escuro |
# MAGIC | 2 | Aba "Mapa Operacional" | Mapa com ~2000 pocos, cores por status |
# MAGIC | 3 | Filtre por campo "Frade" | Somente pocos de Frade aparecem |
# MAGIC | 4 | Aba "Modelo Preditivo" | Formulario com campos preenchidos |
# MAGIC | 5 | Clique "Prever Producao" | Gauge chart com valor em bbl |
# MAGIC | 6 | Aba "Genie Assistant" | Chat funcional, pergunte "Qual campo teve a maior producao?" |
# MAGIC | 7 | Verifique o SQL gerado | SQL valido aparece no painel lateral |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Arquitetura Final
# MAGIC
# MAGIC ```
# MAGIC +------------------------------------------------------------------+
# MAGIC |                    O&G Operations Hub (Databricks App)           |
# MAGIC |                                                                  |
# MAGIC |  +------------------+  +------------------+  +---------------+  |
# MAGIC |  | Mapa Operacional |  | Dashboard AI/BI  |  | Modelo Pred.  |  |
# MAGIC |  | (Plotly Mapbox)  |  | (iframe embed)   |  | (Gauge Chart) |  |
# MAGIC |  +--------+---------+  +--------+---------+  +-------+-------+  |
# MAGIC |           |                     |                     |          |
# MAGIC |  +--------v---------+           |            +--------v-------+  |
# MAGIC |  | SQL Warehouse    |           |            | Model Serving  |  |
# MAGIC |  | (silver_pocos)   |           |            | (RF Champion)  |  |
# MAGIC |  +------------------+           |            +----------------+  |
# MAGIC |                                 |                                |
# MAGIC |  +------------------+  +--------v---------+                      |
# MAGIC |  | Genie Assistant  |  | AI/BI Dashboard  |                      |
# MAGIC |  | (Conversation    |  | (publicado)      |                      |
# MAGIC |  |  API)            |  |                  |                      |
# MAGIC |  +--------+---------+  +------------------+                      |
# MAGIC |           |                                                      |
# MAGIC |  +--------v---------+                                            |
# MAGIC |  | Genie Space      |                                            |
# MAGIC |  | (gold tables)    |                                            |
# MAGIC |  +------------------+                                            |
# MAGIC |                                                                  |
# MAGIC |  Unity Catalog: Governanca em todos os acessos                   |
# MAGIC +------------------------------------------------------------------+
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Parabens!** Voce completou todos os labs do Workshop!
# MAGIC >
# MAGIC > Agora a operadora tem:
# MAGIC > - Pipeline de dados com qualidade (Lab 01)
# MAGIC > - Modelo ML com versionamento (Lab 02)
# MAGIC > - Dashboard executivo e Genie (Lab 03)
# MAGIC > - Aplicacao web integrada (Lab 04)
