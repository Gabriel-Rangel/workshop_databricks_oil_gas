# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Hands-On Databricks - Databricks para Oil & Gas
# MAGIC ## Notebook 00 - Configuracao do Ambiente
# MAGIC
# MAGIC **Objetivo:** Configurar o ambiente isolado para cada participante usando o Unity Catalog.
# MAGIC
# MAGIC O schema de cada participante e criado automaticamente a partir do seu **usuario Databricks**,
# MAGIC sem necessidade de digitar nada.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 1: Identificar o participante automaticamente

# COMMAND ----------

# Extrair nome do usuario a partir do email (antes do @)
# Ex: gabriel.rangel@databricks.com -> gabriel_rangel
usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()

print(f"Usuario: {usuario}")
print(f"Nome derivado: {nome}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 2: Criar o Catalogo (compartilhado) e seu Schema
# MAGIC
# MAGIC ```
# MAGIC workshop_databricks                      <-- Catalogo (compartilhado por todos)
# MAGIC   ├── gabriel_rangel               <-- Schema derivado do email
# MAGIC   │     ├── raw_pocos
# MAGIC   │     ├── bronze_pocos
# MAGIC   │     ├── silver_pocos
# MAGIC   │     ├── gold_producao_por_campo
# MAGIC   │     └── ...
# MAGIC   ├── maria_silva
# MAGIC   └── ...
# MAGIC ```

# COMMAND ----------

CATALOGO = "workshop_databricks"
SCHEMA = nome

# spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Catalogo: {CATALOGO} (compartilhado)")
print(f"Schema:   {SCHEMA} (seu espaco isolado)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 3: Criar Volumes para armazenar arquivos de origem

# COMMAND ----------

for vol in ["pocos_csv", "producao_csv", "ordens_json", "producao_streaming"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{vol}")
    print(f"  /Volumes/{CATALOGO}/{SCHEMA}/{vol}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificacao

# COMMAND ----------

print(f"{'='*60}")
print(f"  AMBIENTE CONFIGURADO COM SUCESSO!")
print(f"{'='*60}")
print(f"  Usuario:   {usuario}")
print(f"  Catalogo:  {CATALOGO}")
print(f"  Schema:    {SCHEMA}")
print(f"  Volumes:   pocos_csv | producao_csv | ordens_json | producao_streaming")
print(f"{'='*60}")
print()
print("Proximo passo: Execute o notebook 01_dados_sinteticos")
