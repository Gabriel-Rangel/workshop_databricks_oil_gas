# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Cleanup
# MAGIC ## Limpeza do Ambiente
# MAGIC
# MAGIC > **ATENCAO:** Este notebook **apaga permanentemente** todas as tabelas e volumes
# MAGIC > do seu schema no workshop.
# MAGIC
# MAGIC ---

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome

print(f"Limpeza do schema: {CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remover seu schema completo (CASCADE)

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOGO}.{SCHEMA} CASCADE")
print(f"Schema {CATALOGO}.{SCHEMA} removido com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpeza manual (UI)
# MAGIC
# MAGIC 1. **Dashboard:** Abra > **(...)** > **Move to trash**
# MAGIC 2. **Genie Space:** Genie > **(...)** > **Move to Trash**
# MAGIC 3. **Pipeline SDP:** Jobs & Pipelines > Pipelines > **(...)** > **Delete**
# MAGIC 4. **Experimento MLflow:** Experiments > `workshop_databricks_producao` > **(...)** > **Delete**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Ambiente limpo!** Obrigado por participar do Workshop Databricks.
