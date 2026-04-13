# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 02: Machine Learning para Oil & Gas
# MAGIC ## Solucao Completa (Referencia)
# MAGIC
# MAGIC > Este notebook contem a **solucao completa** de todos os TO-DOs do Lab 02.
# MAGIC
# MAGIC ---

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Ambiente ML: {CATALOGO}.{SCHEMA}")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import numpy as np

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar e preparar dados

# COMMAND ----------

df_producao = spark.table(f"{CATALOGO}.{SCHEMA}.silver_producao")
df_pocos = spark.table(f"{CATALOGO}.{SCHEMA}.silver_pocos")

df = (
    df_producao
    .join(df_pocos.select("id_poco", "campo", "bacia", "tipo_poco"), "id_poco", "inner")
    .withColumn("semana", weekofyear(col("data_producao")))
    .withColumn("ano", year(col("data_producao")))
)

df_semanal = (
    df.groupBy("campo", "ano", "semana")
    .agg(
        round(sum("vol_oleo_bbl"), 2).alias("vol_oleo_semanal"),
        round(avg("bsw_pct"), 2).alias("bsw_medio"),
        round(avg("pressao_cabeca_psi"), 1).alias("pressao_media"),
        round(avg("temperatura_c"), 1).alias("temperatura_media"),
        round(avg("horas_operacao"), 1).alias("horas_operacao_media"),
        countDistinct("id_poco").alias("pocos_ativos")
    )
    .orderBy("campo", "ano", "semana")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 1 RESOLVIDO: Features de lag e media movel

# COMMAND ----------

window_spec = Window.partitionBy("campo").orderBy("ano", "semana")

df_features = (
    df_semanal
    .withColumn("vol_semana_anterior", lag(col("vol_oleo_semanal"), 1).over(window_spec))
    .withColumn("vol_media_4_semanas", round(avg(col("vol_oleo_semanal")).over(window_spec.rowsBetween(-3, 0)), 2))
    .na.drop()
)

df_features.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparar dados para treinamento

# COMMAND ----------

feature_cols = ["semana", "bsw_medio", "pressao_media", "temperatura_media",
                "horas_operacao_media", "pocos_ativos",
                "vol_semana_anterior", "vol_media_4_semanas"]

target_col = "vol_oleo_semanal"

pdf = df_features.select(feature_cols + [target_col]).toPandas()

X = pdf[feature_cols]
y = pdf[target_col]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Dados de treino: {len(X_train)} | Teste: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 2 RESOLVIDO: Treinamento com MLflow

# COMMAND ----------

from datetime import datetime as dt

EXPERIMENT_PATH = f"/Workspace/Users/{usuario}/workshop_databricks/02_Lab_ML/experimento_{nome}"
mlflow.set_experiment(EXPERIMENT_PATH)
print(f"Experimento MLflow: {EXPERIMENT_PATH}")

run_name = f"producao_decline_{nome}_{dt.now().strftime('%Y%m%d_%H%M%S')}"

mlflow.sklearn.autolog()

with mlflow.start_run(run_name=run_name) as run:
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    mae = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    r2 = r2_score(y_test, predictions)

    mlflow.log_metric("custom_mae", mae)
    mlflow.log_metric("custom_rmse", rmse)
    mlflow.log_metric("custom_r2", r2)

    run_id = run.info.run_id

    print(f"Run ID: {run_id}")
    print(f"MAE:  {mae:,.2f} bbl")
    print(f"RMSE: {rmse:,.2f} bbl")
    print(f"R2:   {r2:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TO-DO 3 RESOLVIDO: Registro no Unity Catalog

# COMMAND ----------

model_name = f"{CATALOGO}.{SCHEMA}.modelo_producao_decline"

from mlflow import MlflowClient
client = MlflowClient()

# Registrar v1 e promover como Champion
result_v1 = mlflow.register_model(f"runs:/{run_id}/model", model_name)
client.set_registered_model_alias(model_name, "champion", result_v1.version)

print(f"Modelo v{result_v1.version} registrado e promovido a CHAMPION")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 5: Treinar modelo Challenger (hiperparametros diferentes)
# MAGIC
# MAGIC Vamos treinar um segundo modelo com parametros diferentes para comparar
# MAGIC com o Champion e decidir se vale promover.

# COMMAND ----------

run_name_v2 = f"producao_decline_challenger_{nome}_{dt.now().strftime('%Y%m%d_%H%M%S')}"

with mlflow.start_run(run_name=run_name_v2) as run_v2:
    # Challenger: mais arvores, profundidade limitada
    model_v2 = RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42)
    model_v2.fit(X_train, y_train)

    predictions_v2 = model_v2.predict(X_test)

    mae_v2 = mean_absolute_error(y_test, predictions_v2)
    rmse_v2 = np.sqrt(mean_squared_error(y_test, predictions_v2))
    r2_v2 = r2_score(y_test, predictions_v2)

    mlflow.log_metric("custom_mae", mae_v2)
    mlflow.log_metric("custom_rmse", rmse_v2)
    mlflow.log_metric("custom_r2", r2_v2)

    run_id_v2 = run_v2.info.run_id

    print(f"Challenger - Run ID: {run_id_v2}")
    print(f"MAE:  {mae_v2:,.2f} bbl")
    print(f"RMSE: {rmse_v2:,.2f} bbl")
    print(f"R2:   {r2_v2:.4f}")

# Registrar como v2
result_v2 = mlflow.register_model(f"runs:/{run_id_v2}/model", model_name)
client.set_registered_model_alias(model_name, "challenger", result_v2.version)

print(f"\nModelo v{result_v2.version} registrado como CHALLENGER")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 6: Comparar Champion vs Challenger

# COMMAND ----------

print(f"{'='*60}")
print(f"  CHAMPION (v{result_v1.version}) vs CHALLENGER (v{result_v2.version})")
print(f"{'='*60}")
print(f"  {'Metrica':<12} {'Champion':>12} {'Challenger':>12} {'Melhor':>10}")
print(f"  {'-'*46}")
print(f"  {'MAE':<12} {mae:>12,.2f} {mae_v2:>12,.2f} {'Challenger' if mae_v2 < mae else 'Champion':>10}")
print(f"  {'RMSE':<12} {rmse:>12,.2f} {rmse_v2:>12,.2f} {'Challenger' if rmse_v2 < rmse else 'Champion':>10}")
print(f"  {'R2':<12} {r2:>12.4f} {r2_v2:>12.4f} {'Challenger' if r2_v2 > r2 else 'Champion':>10}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 7: Promover Challenger (decisao manual)
# MAGIC
# MAGIC No Unity Catalog, a promocao de modelos usa **aliases**:
# MAGIC - `champion`: modelo em producao
# MAGIC - `challenger`: candidato sendo avaliado
# MAGIC
# MAGIC > **IMPORTANTE:** A decisao de promover um modelo NAO deve ser automatica.
# MAGIC > As metricas offline (MAE, RMSE, R2) sao apenas uma parte da avaliacao.
# MAGIC > Em producao, voce tambem precisa considerar:
# MAGIC > - Testes A/B com dados reais
# MAGIC > - Validacao de dominio com engenheiros de producao
# MAGIC > - Analise de bias e fairness
# MAGIC > - Performance em cenarios edge-case (pocos novos, paradas, etc.)
# MAGIC >
# MAGIC > Por isso a promocao e uma **decisao humana**, nao automatica.

# COMMAND ----------

# Para promover o Challenger a Champion, execute a celula abaixo MANUALMENTE:
# (descomente a linha e execute)

# client.set_registered_model_alias(model_name, "champion", result_v2.version)
# print(f"Challenger v{result_v2.version} PROMOVIDO a Champion!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 8: Inferencia pontual com o modelo Champion
# MAGIC
# MAGIC Em producao, voce sempre carrega o modelo pelo alias `champion`,
# MAGIC independente da versao — assim a troca e transparente.

# COMMAND ----------

import mlflow.pyfunc
import pandas as pd

# Carregar o modelo champion do Unity Catalog
model_champion = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

# Fazer inferencia nos dados de teste
predicoes = model_champion.predict(X_test)

# Mostrar resultado (usar numpy abs para evitar conflito com pyspark abs)
df_resultado = pd.DataFrame({
    "real_bbl": y_test.values,
    "previsto_bbl": predicoes,
    "erro_bbl": np.abs(y_test.values - predicoes)
}).sort_values("erro_bbl", ascending=False)

print(f"Inferencia com modelo Champion ({model_name}@champion)")
print(f"MAE: {df_resultado['erro_bbl'].mean():,.2f} bbl\n")
display(spark.createDataFrame(df_resultado))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 9: Inferencia em batch (Spark DataFrame)
# MAGIC
# MAGIC Para inferencia em larga escala, carregamos o modelo como UDF do Spark
# MAGIC e aplicamos diretamente sobre um DataFrame distribuido — sem coletar no driver.

# COMMAND ----------

# Carregar modelo como Spark UDF
predict_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_name}@champion", result_type="double")

# Aplicar sobre o DataFrame de features completo (distribuido)
df_batch_inference = (
    df_features
    .withColumn("producao_prevista_bbl", predict_udf(*[col(c) for c in feature_cols]))
    .select("campo", "ano", "semana", "vol_oleo_semanal", "producao_prevista_bbl")
    .withColumn("erro_bbl", col("vol_oleo_semanal") - col("producao_prevista_bbl"))
)

print(f"Inferencia batch com {model_name}@champion sobre {df_batch_inference.count()} registros")
df_batch_inference.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo
# MAGIC
# MAGIC | Etapa | O que fizemos | Ferramenta |
# MAGIC |-------|-------------|------------|
# MAGIC | Feature Engineering | Lag, media movel, features temporais | PySpark Window Functions |
# MAGIC | Preparacao de dados | Conversao para pandas (dados agregados) | .toPandas() |
# MAGIC | Treinamento v1 | RandomForest (n=100) → Champion | scikit-learn + MLflow |
# MAGIC | Treinamento v2 | RandomForest (n=200, depth=10) → Challenger | scikit-learn + MLflow |
# MAGIC | Comparacao | Champion vs Challenger (MAE, RMSE, R2) | MLflow metrics |
# MAGIC | Promocao | Mover alias `champion` para melhor versao | MlflowClient aliases |
# MAGIC | Inferencia | Carregar modelo por alias e prever | mlflow.pyfunc.load_model |
# MAGIC
# MAGIC ### Aliases no Unity Catalog
# MAGIC
# MAGIC | Alias | O que significa | Como usar |
# MAGIC |-------|----------------|-----------|
# MAGIC | `champion` | Modelo em producao | `models:/{nome}@champion` |
# MAGIC | `challenger` | Candidato a producao | `models:/{nome}@challenger` |
# MAGIC
# MAGIC > Para promover: `client.set_registered_model_alias(model_name, "champion", version)`
# MAGIC >
# MAGIC > Para carregar: `mlflow.pyfunc.load_model(f"models:/{model_name}@champion")`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Proximo:** Lab 03 (03_Lab_AIBI) para criar Dashboards e Genie!
