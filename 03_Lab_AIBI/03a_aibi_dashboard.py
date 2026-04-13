# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 03: AI/BI Dashboards
# MAGIC ## Parte A: Dashboard de Producao
# MAGIC
# MAGIC **Objetivo:** Criar um dashboard executivo com os KPIs de producao da operadora
# MAGIC usando os **AI/BI Dashboards** nativos do Databricks.
# MAGIC
# MAGIC ### O que diferencia os dashboards do Databricks?
# MAGIC - **IA integrada:** descreva o grafico em linguagem natural e o Databricks cria
# MAGIC - **Governanca nativa:** permissoes do Unity Catalog se aplicam automaticamente
# MAGIC - **Sem duplicacao de dados:** le diretamente das tabelas Gold/Silver
# MAGIC
# MAGIC ---

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 1: Criar um novo Dashboard
# MAGIC
# MAGIC 1. No **menu lateral**, clique em **+ New** > **Dashboard**
# MAGIC 2. Renomeie para: `O&G - Painel de Producao ({seu_nome})`
# MAGIC
# MAGIC Abas importantes:
# MAGIC - **Canvas**: organizacao visual dos graficos
# MAGIC - **Data**: datasets (queries SQL) que alimentam os graficos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 2: Criar datasets
# MAGIC
# MAGIC Na aba **Data**, clique em **+ Create dataset** e adicione as queries abaixo.
# MAGIC
# MAGIC ---
# MAGIC #### Dataset 1: KPIs Executivos
# MAGIC > Tipo sugerido: **Counter / KPI cards**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(DISTINCT id_poco) FROM silver_pocos WHERE status = 'Produzindo') AS pocos_produzindo,
# MAGIC   (SELECT ROUND(SUM(vol_oleo_bbl), 0) FROM silver_producao) AS producao_total_bbl,
# MAGIC   (SELECT ROUND(AVG(bsw_pct), 1) FROM silver_producao) AS bsw_medio_pct,
# MAGIC   (SELECT COUNT(*) FROM silver_ordens WHERE status = 'Aberta') AS ordens_abertas

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataset 2: Producao por Campo
# MAGIC > Tipo sugerido: **Barra horizontal**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   campo,
# MAGIC   pocos_ativos,
# MAGIC   producao_total_bbl,
# MAGIC   bsw_medio_pct
# MAGIC FROM gold_producao_por_campo
# MAGIC ORDER BY producao_total_bbl DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataset 3: Evolucao Mensal da Producao
# MAGIC > Tipo sugerido: **Linha** (eixo X: ano_mes, eixo Y: producao_mensal)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(data_producao, 'yyyy-MM') AS ano_mes,
# MAGIC   ROUND(SUM(vol_oleo_bbl), 2) AS producao_mensal_bbl,
# MAGIC   ROUND(SUM(vol_gas_mm3), 2) AS gas_mensal_mm3,
# MAGIC   ROUND(AVG(bsw_pct), 2) AS bsw_medio
# MAGIC FROM silver_producao
# MAGIC GROUP BY DATE_FORMAT(data_producao, 'yyyy-MM')
# MAGIC ORDER BY ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataset 4: Top 10 Ordens de Manutencao por Custo
# MAGIC > Tipo sugerido: **Tabela detalhada**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   o.numero_om_sap,
# MAGIC   o.tipo_manutencao,
# MAGIC   o.equipamento,
# MAGIC   o.custo_estimado_brl,
# MAGIC   o.custo_real_brl,
# MAGIC   o.prioridade,
# MAGIC   o.status,
# MAGIC   p.campo
# MAGIC FROM silver_ordens o
# MAGIC LEFT JOIN silver_pocos p ON o.id_poco = p.id_poco
# MAGIC ORDER BY o.custo_estimado_brl DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataset 5: BSW vs Producao (Saude do Poco)
# MAGIC > Tipo sugerido: **Scatter plot** (X: producao media, Y: BSW medio, cor: campo)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.id_poco,
# MAGIC   p.campo,
# MAGIC   p.tipo_poco,
# MAGIC   ROUND(AVG(pr.vol_oleo_bbl), 2) AS producao_media_bbl,
# MAGIC   ROUND(AVG(pr.bsw_pct), 2) AS bsw_medio_pct,
# MAGIC   ROUND(AVG(pr.pressao_cabeca_psi), 1) AS pressao_media_psi
# MAGIC FROM silver_producao pr
# MAGIC JOIN silver_pocos p ON pr.id_poco = p.id_poco
# MAGIC GROUP BY p.id_poco, p.campo, p.tipo_poco
# MAGIC HAVING COUNT(*) > 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 3: Criar visualizacoes no Canvas
# MAGIC
# MAGIC Na aba **Canvas**:
# MAGIC 1. Clique em **Add a visualization**
# MAGIC 2. Opcoes:
# MAGIC    - **IA assistida:** "grafico de barras mostrando producao por campo"
# MAGIC    - **Manual:** selecione dataset, tipo de grafico, eixos
# MAGIC 3. Arraste e redimensione no canvas
# MAGIC
# MAGIC ### Passo 4: Adicionar filtros
# MAGIC
# MAGIC 1. Clique em **Add a filter**
# MAGIC 2. Adicione filtros por: `campo`, `tipo_poco`, `ano_mes`
# MAGIC 3. Marque em quais graficos cada filtro se aplica
# MAGIC
# MAGIC ### Passo 5: Publicar
# MAGIC
# MAGIC 1. Clique em **Publish** para publicar o dashboard
# MAGIC 2. **Share** para compartilhar com outros usuarios
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Proximo:** Notebook **`03b_aibi_genie`** para criar o Genie Space!
