# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 01: Validacao do Pipeline
# MAGIC ## Contagem de registros e versoes das tabelas
# MAGIC
# MAGIC **Objetivo:** Verificar o estado do pipeline — quantos registros cada tabela tem
# MAGIC e qual a versao atual (Delta Lake history).
# MAGIC
# MAGIC > **Como usar:**
# MAGIC > 1. Execute apos criar o pipeline para ver os dados iniciais
# MAGIC > 2. Inicie o gerador de streaming (`01a_gerador_streaming`)
# MAGIC > 3. Execute novamente — observe os novos registros e novas versoes!
# MAGIC >
# MAGIC > Isso demonstra o **Auto Loader** processando incrementalmente e o
# MAGIC > **Delta Lake** versionando cada micro-batch.
# MAGIC
# MAGIC ---

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Validando: {CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contagem de registros por tabela

# COMMAND ----------

tabelas = [
    "bronze_pocos", "bronze_producao", "bronze_ordens",
    "silver_pocos", "silver_producao", "silver_ordens",
    "gold_producao_por_campo", "gold_custos_manutencao", "gold_eficiencia_operacional"
]

resultados = []
for tabela in tabelas:
    try:
        count = spark.table(f"{CATALOGO}.{SCHEMA}.{tabela}").count()
        camada = tabela.split("_")[0].upper()
        resultados.append((camada, tabela, count))
    except Exception as e:
        resultados.append(("?", tabela, f"Nao encontrada"))

df_contagem = spark.createDataFrame(resultados, ["camada", "tabela", "registros"])
df_contagem.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Historico de versoes (Delta Lake Time Travel)
# MAGIC
# MAGIC Cada vez que o pipeline processa novos dados, o Delta Lake cria uma nova **versao**.
# MAGIC Isso permite:
# MAGIC - **Auditoria:** saber quando cada batch foi processado
# MAGIC - **Rollback:** voltar para uma versao anterior se algo der errado
# MAGIC - **Debug:** comparar dados entre versoes

# COMMAND ----------

for tabela in tabelas:
    try:
        print(f"\n{'='*60}")
        print(f"  DESCRIBE HISTORY {tabela}")
        print(f"{'='*60}")
        spark.sql(f"DESCRIBE HISTORY {CATALOGO}.{SCHEMA}.{tabela}").select(
            "version", "timestamp", "operation", "operationMetrics"
        ).display()
    except:
        print(f"  {tabela}: tabela nao encontrada (pipeline ainda nao criou)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo rapido

# COMMAND ----------

print(f"\n{'='*60}")
print(f"  RESUMO - {CATALOGO}.{SCHEMA}")
print(f"{'='*60}")
for camada, tabela, registros in resultados:
    status = f"{registros:>10}" if isinstance(registros, int) else f"{'N/A':>10}"
    print(f"  [{camada:6}] {tabela:<40} {status} registros")
print(f"{'='*60}")
print(f"\n  Dica: Execute novamente apos iniciar o streaming para ver os novos dados!")
