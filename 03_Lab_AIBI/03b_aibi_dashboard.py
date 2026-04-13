# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 03: AI/BI
# MAGIC ## Parte B: Dashboard de Producao (Importar JSON)
# MAGIC
# MAGIC **Objetivo:** Importar um dashboard AI/BI pre-configurado com os KPIs
# MAGIC de producao da operadora.
# MAGIC
# MAGIC O dashboard ja foi criado e exportado como JSON. Voce so precisa importar
# MAGIC e conectar ao seu schema.
# MAGIC
# MAGIC ---

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome

print(f"Ambiente: {CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 1: Preparar o arquivo JSON
# MAGIC
# MAGIC Antes de importar, faca duas alteracoes no arquivo:
# MAGIC
# MAGIC **1a. Renomear o arquivo** (o nome do arquivo vira o nome do dashboard):
# MAGIC 1. No workspace, navegue ate `03_Lab_AIBI/`
# MAGIC 2. Clique com botao direito no arquivo `OIL&GAS — Painel Operacional de Produção.lvdash.json`
# MAGIC 3. Selecione **Rename**
# MAGIC 4. Renomeie para: `O&G - Painel Operacional ({seu_nome}).lvdash.json`
# MAGIC    - Exemplo: `O&G - Painel Operacional (gabriel_rangel).lvdash.json`
# MAGIC
# MAGIC **1b. Substituir o schema** nas queries do JSON:
# MAGIC 1. Abra o arquivo renomeado
# MAGIC 2. Use **Find & Replace** (Ctrl+H / Cmd+H) para substituir o schema:
# MAGIC    - **Buscar:** o schema original (ex: `gabriel_rangel`)
# MAGIC    - **Substituir por:** `{seu_schema}` (o seu nome derivado do email)
# MAGIC
# MAGIC **1c. Substituir o Genie Space ID** (o dashboard tem Genie embutido):
# MAGIC 1. Ainda no mesmo arquivo, busque o `genie_space_id` original
# MAGIC 2. Substitua pelo ID do Genie Space que voce criou no passo anterior (03a)
# MAGIC    - Para encontrar o ID: abra o Genie Space e copie o ID da URL
# MAGIC    - Ex: `https://workspace.azuredatabricks.net/genie/rooms/01f0abcd1234...` → o ID e `01f0abcd1234...`
# MAGIC 3. Salve o arquivo
# MAGIC
# MAGIC ### Passo 2: Importar o Dashboard
# MAGIC
# MAGIC 1. No **menu lateral**, clique em **Dashboards**
# MAGIC 2. No canto superior direito, clique no botao **kebab (...)** ou **Import**
# MAGIC 3. Selecione **Import dashboard from file**
# MAGIC 4. Selecione o arquivo JSON que voce acabou de renomear e editar
# MAGIC 5. O dashboard sera criado automaticamente com o nome do arquivo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 4: Validar os dados
# MAGIC
# MAGIC 1. Clique em **Edit** (icone de lapis no canto superior direito)
# MAGIC 2. Na aba **Data** (parte inferior), clique em cada dataset
# MAGIC 3. Clique em **Run** para validar que as queries estao retornando dados
# MAGIC 4. Se algum grafico nao carregar, verifique se o schema esta correto na query

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 5: Explorar o Dashboard
# MAGIC
# MAGIC O dashboard contem:
# MAGIC
# MAGIC | Visualizacao | Descricao |
# MAGIC |-------------|-----------|
# MAGIC | **KPI Cards** | Pocos produzindo, producao total, BSW medio, ordens abertas |
# MAGIC | **Producao por Campo** | Barras com volume de oleo por campo (Frade, Albacora Leste, etc.) |
# MAGIC | **Evolucao Mensal** | Linha temporal da producao ao longo dos meses |
# MAGIC | **Custos de Manutencao** | Top custos por tipo de equipamento |
# MAGIC | **Saude dos Pocos** | BSW vs producao (scatter plot) |
# MAGIC
# MAGIC ### O que observar:
# MAGIC - **Filtros interativos**: campo, tipo de poco, periodo
# MAGIC - **Governanca**: as permissoes do Unity Catalog se aplicam — se o usuario nao tem acesso a tabela, o grafico nao carrega
# MAGIC - **IA assistida**: no modo Edit, voce pode pedir ao Databricks para criar novos graficos em linguagem natural

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 6: Publicar e Compartilhar
# MAGIC
# MAGIC 1. Clique em **Publish** no canto superior direito
# MAGIC 2. Para compartilhar: **Share** > adicione usuarios ou grupos
# MAGIC 3. O link do dashboard pode ser acessado por qualquer pessoa com permissao

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo Final do Workshop
# MAGIC
# MAGIC ### O que construimos juntos
# MAGIC
# MAGIC ```
# MAGIC +-----------------------------------------------------------------+
# MAGIC |              O&G - DATA LAKEHOUSE                               |
# MAGIC |                                                                  |
# MAGIC |  SAP HANA (PM, PP, FI/CO) + SCADA/PI Historian                  |
# MAGIC |    |                                                             |
# MAGIC |    v  Auto Loader / Lakeflow Connect                             |
# MAGIC |  Bronze (Delta Lake) -- dados brutos + metadados                 |
# MAGIC |    |                                                             |
# MAGIC |    v  SDP Pipeline + Data Quality Expectations                   |
# MAGIC |  Silver (Delta Lake) -- dados limpos e validados                 |
# MAGIC |    |                                                             |
# MAGIC |    v  Agregacoes de negocio                                      |
# MAGIC |  Gold (Delta Lake) -- KPIs de producao e manutencao              |
# MAGIC |    |                                                             |
# MAGIC |    +-> Genie            (perguntas em linguagem natural)         |
# MAGIC |    +-> AI/BI Dashboard  (painel executivo)                       |
# MAGIC |    +-> ML / MLflow      (previsao de declinio + model serving)   |
# MAGIC |    +-> Databricks App   (aplicacao para engenheiros)             |
# MAGIC |                                                                  |
# MAGIC |  Unity Catalog: Governanca em TODAS as camadas                   |
# MAGIC +-----------------------------------------------------------------+
# MAGIC ```
# MAGIC
# MAGIC ### Labs do Workshop
# MAGIC
# MAGIC | Lab | Conteudo | Duracao |
# MAGIC |-----|---------|---------|
# MAGIC | 00 Setup | Catalogo + dados sinteticos O&G/SAP | 15 min |
# MAGIC | 01 SDP | Auto Loader, Pipeline Bronze/Silver/Gold, DQ Expectations | 40 min |
# MAGIC | 02 ML | Declinio de producao, MLflow, Champion/Challenger, Inferencia | 35 min |
# MAGIC | 03 AI/BI | Genie Space + Dashboard (importar JSON) | 25 min |
# MAGIC
# MAGIC ### Proximos passos sugeridos
# MAGIC
# MAGIC | # | O que fazer | Por que |
# MAGIC |---|-----------|---------|
# MAGIC | 1 | **POC com dados reais** | Conectar ao SAP HANA da operadora via Lakeflow Connect |
# MAGIC | 2 | **Integrar SCADA/PI** | Streaming real de dados de sensores de pocos |
# MAGIC | 3 | **Expandir ML** | Modelos de otimizacao de EOR (Enhanced Oil Recovery) |
# MAGIC | 4 | **Governanca** | Column Masks para dados sensiveis (ANP), Row-Level Security por campo |
# MAGIC | 5 | **Model Serving** | Deploy do modelo de declinio para previsoes em tempo real |
# MAGIC | 6 | **Databricks Apps** | Aplicacao web para engenheiros de producao |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Obrigado por participar do Workshop!**
# MAGIC >
# MAGIC > Duvidas? Entre em contato com seu Solutions Architect.
