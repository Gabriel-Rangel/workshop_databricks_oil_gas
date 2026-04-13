# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 03: AI/BI
# MAGIC ## Parte A: Genie Space - Assistente de Producao
# MAGIC
# MAGIC **Objetivo:** Criar um **Genie Space** que permite a qualquer pessoa da operadora
# MAGIC fazer perguntas sobre producao e manutencao usando **linguagem natural** — sem SQL.
# MAGIC
# MAGIC ### Para quem e o Genie?
# MAGIC - **Gerentes de campo** que querem ver KPIs sem depender de analistas
# MAGIC - **Engenheiros de producao** que precisam de respostas rapidas
# MAGIC - **Diretoria** que quer acompanhar performance dos ativos
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
# MAGIC ### Passo 1: Criar o Genie Space
# MAGIC
# MAGIC 1. No **menu lateral**, clique em **Genie**
# MAGIC 2. Clique em **New**
# MAGIC 3. Adicione as tabelas que o Genie tera acesso:
# MAGIC    - `workshop_databricks.{seu_schema}.gold_producao_por_campo`
# MAGIC    - `workshop_databricks.{seu_schema}.gold_custos_manutencao`
# MAGIC    - `workshop_databricks.{seu_schema}.gold_eficiencia_operacional`
# MAGIC    - `workshop_databricks.{seu_schema}.silver_pocos`
# MAGIC    - `workshop_databricks.{seu_schema}.silver_producao`
# MAGIC    - `workshop_databricks.{seu_schema}.silver_ordens`
# MAGIC 4. Clique em **Create**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 2: Configurar o Genie Space
# MAGIC
# MAGIC 1. Clique em **Configure** > **Settings**
# MAGIC 2. Preencha:
# MAGIC    - **Title:** `O&G - Assistente de Producao ({seu_nome})`
# MAGIC    - **Description:** `Assistente para explorar dados de producao, pocos e manutencao da operadora.`
# MAGIC    - **Default warehouse:** selecione um SQL Warehouse disponivel
# MAGIC
# MAGIC 3. Em **Instructions** > **Text**, cole:
# MAGIC
# MAGIC ```
# MAGIC Voce e o assistente de dados da operadora S.A., a maior petroleira independente do Brasil.
# MAGIC
# MAGIC Contexto de negocio:
# MAGIC - A operadora opera campos offshore: Frade, Tubarao Martelo, Polvo, Albacora Leste, Wahoo
# MAGIC - Bacia de Campos e Santos sao as principais areas de operacao
# MAGIC - BSW (Basic Sediment & Water) acima de 80% indica poco com problemas
# MAGIC - Producao e medida em barris por dia (bbl/dia)
# MAGIC - Ordens de manutencao vem do SAP PM
# MAGIC
# MAGIC Regras:
# MAGIC - Sempre responda em portugues do Brasil
# MAGIC - Formate valores monetarios em Reais (R$)
# MAGIC - Formate volumes de producao em barris (bbl)
# MAGIC - Quando perguntarem sobre "eficiencia", use a tabela gold_eficiencia_operacional
# MAGIC - Priorize tabelas gold_ para respostas agregadas
# MAGIC ```
# MAGIC
# MAGIC 4. Adicione **Sample questions**:
# MAGIC    - `Qual campo teve a maior producao total?`
# MAGIC    - `Quanto gastamos em manutencao no campo de Frade?`
# MAGIC    - `Quais pocos estao com BSW acima de 80%?`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 3: Testar o Genie Space
# MAGIC
# MAGIC Agora va para a area de chat e teste estas perguntas:
# MAGIC
# MAGIC | # | Pergunta | O que esperar |
# MAGIC |---|----------|--------------|
# MAGIC | 1 | "Qual campo teve a maior producao?" | Ranking por campo |
# MAGIC | 2 | "Quanto gastamos em manutencao no campo de Frade?" | Custo total filtrado |
# MAGIC | 3 | "Mostre a evolucao mensal da producao de Albacora Leste" | Grafico de linha temporal |
# MAGIC | 4 | "Quais pocos estao com BSW acima de 80%?" | Lista de pocos com BSW alto |
# MAGIC | 5 | "Quantos pocos estao produzindo por bacia?" | Contagem agrupada |
# MAGIC | 6 | "Qual tipo de manutencao e mais caro?" | Comparativo de custos |
# MAGIC | 7 | "Mostre os 5 maiores custos de manutencao" | Top 5 por valor |
# MAGIC
# MAGIC ### O que observar:
# MAGIC - O Genie **mostra o SQL gerado** — verifique se esta correto
# MAGIC - O resultado vem com **visualizacao automatica**
# MAGIC - Faca **follow-up questions** na mesma conversa
# MAGIC - O Genie respeita as **permissoes do Unity Catalog**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Proximo:** Notebook **`03b_aibi_dashboard`** para importar o dashboard de producao!
