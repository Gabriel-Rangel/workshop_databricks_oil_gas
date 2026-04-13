# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 02: Machine Learning para Oil & Gas
# MAGIC ## Exercicios (To-Do)
# MAGIC
# MAGIC **Objetivo:** Aplicar Machine Learning a dados de producao da operadora
# MAGIC usando **PySpark, Pandas API on Spark, scikit-learn e MLflow**.
# MAGIC
# MAGIC ### O que vamos construir
# MAGIC
# MAGIC | Etapa | O que faz | Tecnica |
# MAGIC |-------|----------|---------|
# MAGIC | **Feature Engineering** | Criar features temporais e de lag | PySpark Window Functions |
# MAGIC | **Treinamento** | Prever declinio de producao semanal | RandomForest + MLflow |
# MAGIC | **Registro** | Governar o modelo no Unity Catalog | MLflow Model Registry |
# MAGIC
# MAGIC ### Contexto de negocio
# MAGIC A operadora adquire campos maduros e precisa prever o **declinio natural de producao**
# MAGIC para planejar investimentos (workovers, novos pocos) e reportar projecoes para a **ANP**.
# MAGIC
# MAGIC ### Exercicios (TO-DOs)
# MAGIC Voce tem **3 exercicios** para completar. Procure pelos blocos `TO-DO`.
# MAGIC
# MAGIC > **Dica:** Consulte `02b_ml_completo` se travar.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuracao

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Ambiente ML: {CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import numpy as np

# Configurar MLflow para usar Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 1: Carregar e preparar os dados

# COMMAND ----------

# Ler dados da Silver (criados no Lab 01)
df_producao = spark.table(f"{CATALOGO}.{SCHEMA}.silver_producao")
df_pocos = spark.table(f"{CATALOGO}.{SCHEMA}.silver_pocos")

# Juntar producao com informacoes do poco
df = (
    df_producao
    .join(df_pocos.select("id_poco", "campo", "bacia", "tipo_poco"), "id_poco", "inner")
    .withColumn("semana", weekofyear(col("data_producao")))
    .withColumn("ano", year(col("data_producao")))
)

print(f"Registros de producao: {df.count()}")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2: Agregar por semana e campo

# COMMAND ----------

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

df_semanal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### **TO-DO 1**: Criar features de lag e rolling average
# MAGIC
# MAGIC Use **Window Functions** para criar:
# MAGIC - `vol_semana_anterior`: volume de oleo da semana anterior (lag de 1)
# MAGIC - `vol_media_4_semanas`: media movel das ultimas 4 semanas
# MAGIC
# MAGIC > **Dica:**
# MAGIC > ```python
# MAGIC > window_spec = Window.partitionBy("campo").orderBy("ano", "semana")
# MAGIC > lag(col("vol_oleo_semanal"), 1).over(window_spec)
# MAGIC > avg(col("vol_oleo_semanal")).over(window_spec.rowsBetween(-3, 0))
# MAGIC > ```

# COMMAND ----------

window_spec = Window.partitionBy("campo").orderBy("ano", "semana")

df_features = df_semanal

# ============================================================
# TO-DO 1: Adicione as colunas de lag e media movel abaixo
#
# 1. vol_semana_anterior = lag de 1 semana do vol_oleo_semanal
# 2. vol_media_4_semanas = media movel das ultimas 4 semanas
#
# Dica: use lag() e avg().over(window_spec.rowsBetween(-3, 0))
# ============================================================

# df_features = (
#     df_semanal
#     .withColumn("vol_semana_anterior", lag(col("vol_oleo_semanal"), 1).over(window_spec))
#     .withColumn("vol_media_4_semanas", round(avg(col("vol_oleo_semanal")).over(window_spec.rowsBetween(-3, 0)), 2))
#     .na.drop()  # Remover linhas com null (primeiras semanas sem lag)
# )

df_features.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 3: Preparar dados para treinamento
# MAGIC
# MAGIC Usamos `.toPandas()` para converter o Spark DataFrame em pandas nativo.
# MAGIC Isso e seguro aqui porque os dados ja estao agregados (poucas centenas de linhas).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pandas nativo vs. Pandas API on Spark
# MAGIC
# MAGIC O Databricks oferece a **Pandas API on Spark** (`pyspark.pandas`) que permite
# MAGIC usar sintaxe pandas sobre dados distribuidos. Porem existem diferencas importantes:
# MAGIC
# MAGIC | Aspecto | Pandas nativo (`.toPandas()`) | Pandas API on Spark (`pyspark.pandas`) |
# MAGIC |---------|------------------------------|----------------------------------------|
# MAGIC | **Onde executa** | Driver (memoria unica) | Distribuido nos workers |
# MAGIC | **Ordenacao** | Deterministica (preserva ordem) | Nao garantida (distribuido) |
# MAGIC | **Precisao float** | Resultado unico | Pode variar por ordem de agregacao |
# MAGIC | **Nulls** | NaN (numpy) | None/NULL (Spark) |
# MAGIC | **Index** | RangeIndex deterministico | Index distribuido |
# MAGIC | **Compatibilidade sklearn** | Nativa | Requer `.to_pandas()` para treinar |
# MAGIC
# MAGIC **Quando usar Pandas API on Spark:**
# MAGIC - Feature engineering em **milhoes de linhas** (nao cabe no driver)
# MAGIC - Limpeza e transformacao distribuida com sintaxe familiar
# MAGIC - Operacoes que nao dependem de ordem (groupby, agg, filter)
# MAGIC
# MAGIC **Quando usar Pandas nativo:**
# MAGIC - Dados agregados/pequenos (como neste caso)
# MAGIC - Treinamento com scikit-learn (requer pandas nativo)
# MAGIC - Operacoes que dependem de ordem deterministica (train_test_split, iloc)
# MAGIC
# MAGIC > **Documentacao:** [Pandas API on Spark](https://docs.databricks.com/aws/en/pandas/pandas-on-spark)

# COMMAND ----------

feature_cols = ["semana", "bsw_medio", "pressao_media", "temperatura_media",
                "horas_operacao_media", "pocos_ativos"]

# Descomente a linha abaixo se completou o TO-DO 1:
# feature_cols += ["vol_semana_anterior", "vol_media_4_semanas"]

target_col = "vol_oleo_semanal"

pdf = df_features.select(feature_cols + [target_col]).toPandas()

X = pdf[feature_cols]
y = pdf[target_col]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Dados de treino: {len(X_train)} | Teste: {len(X_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### **TO-DO 2**: Treinar o modelo com MLflow
# MAGIC
# MAGIC 1. Ative o `mlflow.sklearn.autolog()`
# MAGIC 2. Dentro de um `mlflow.start_run()`, treine um `RandomForestRegressor`
# MAGIC 3. Calcule e logue as metricas: MAE, RMSE, R2
# MAGIC
# MAGIC > **Dica:**
# MAGIC > ```python
# MAGIC > mlflow.sklearn.autolog()
# MAGIC > with mlflow.start_run(run_name="producao_decline_rf"):
# MAGIC >     model = RandomForestRegressor(n_estimators=100, random_state=42)
# MAGIC >     model.fit(X_train, y_train)
# MAGIC >     predictions = model.predict(X_test)
# MAGIC >     mae = mean_absolute_error(y_test, predictions)
# MAGIC >     mlflow.log_metric("custom_mae", mae)
# MAGIC > ```

# COMMAND ----------

# Configurar o experimento MLflow (dentro da pasta do Lab ML, isolado por usuario)
from datetime import datetime as dt

EXPERIMENT_PATH = f"/Workspace/Users/{usuario}/workshop_databricks/02_Lab_ML/experimento_{nome}"
mlflow.set_experiment(EXPERIMENT_PATH)
print(f"Experimento MLflow: {EXPERIMENT_PATH}")

# Nome do run unico: usuario + timestamp
run_name = f"producao_decline_{nome}_{dt.now().strftime('%Y%m%d_%H%M%S')}"

# ============================================================
# TO-DO 2: Treine o modelo com MLflow tracking
#
# 1. Ative autolog: mlflow.sklearn.autolog()
# 2. Abra um run: with mlflow.start_run(run_name=run_name):
# 3. Treine: model = RandomForestRegressor(n_estimators=100, random_state=42)
# 4. Faca predicoes: predictions = model.predict(X_test)
# 5. Calcule metricas: mae, rmse, r2
# 6. Logue metricas extras: mlflow.log_metric("custom_mae", mae)
# ============================================================

# mlflow.sklearn.autolog()
#
# with mlflow.start_run(run_name=run_name) as run:
#     model = RandomForestRegressor(n_estimators=100, random_state=42)
#     model.fit(X_train, y_train)
#
#     predictions = model.predict(X_test)
#
#     mae = mean_absolute_error(y_test, predictions)
#     rmse = np.sqrt(mean_squared_error(y_test, predictions))
#     r2 = r2_score(y_test, predictions)
#
#     mlflow.log_metric("custom_mae", mae)
#     mlflow.log_metric("custom_rmse", rmse)
#     mlflow.log_metric("custom_r2", r2)
#
#     run_id = run.info.run_id
#
#     print(f"Run ID: {run_id}")
#     print(f"MAE:  {mae:,.2f} bbl")
#     print(f"RMSE: {rmse:,.2f} bbl")
#     print(f"R2:   {r2:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 4: Registro de Modelo no Unity Catalog
# MAGIC
# MAGIC Registrar modelos no Unity Catalog permite:
# MAGIC - **Versionamento**: rastrear evolucao do modelo ao longo do tempo
# MAGIC - **Governanca**: controlar quem pode usar o modelo em producao
# MAGIC - **Lineage**: saber quais dados foram usados para treinar
# MAGIC - **Deployment**: servir o modelo via Model Serving
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **TO-DO 3**: Registrar o modelo no Unity Catalog
# MAGIC
# MAGIC Registre o modelo treinado no TO-DO 2 no Unity Catalog.
# MAGIC
# MAGIC > **Dica:**
# MAGIC > ```python
# MAGIC > model_name = f"{CATALOGO}.{SCHEMA}.modelo_producao_decline"
# MAGIC > result = mlflow.register_model(f"runs:/{run_id}/model", model_name)
# MAGIC > ```

# COMMAND ----------

# ============================================================
# TO-DO 3: Registrar o modelo e promover como Champion
#
# 1. Defina: model_name = f"{CATALOGO}.{SCHEMA}.modelo_producao_decline"
# 2. Registre: result = mlflow.register_model(f"runs:/{run_id}/model", model_name)
# 3. Promova com alias:
#    from mlflow import MlflowClient
#    client = MlflowClient()
#    client.set_registered_model_alias(model_name, "champion", result.version)
#
# Exemplo completo:
# model_name = f"{CATALOGO}.{SCHEMA}.modelo_producao_decline"
# from mlflow import MlflowClient
# client = MlflowClient()
#
# result = mlflow.register_model(f"runs:/{run_id}/model", model_name)
# client.set_registered_model_alias(model_name, "champion", result.version)
# print(f"Modelo v{result.version} registrado e promovido a CHAMPION")
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 5: Treinar modelo Challenger
# MAGIC
# MAGIC Treine um segundo modelo com **hiperparametros diferentes** para comparar
# MAGIC com o Champion. Registre como `challenger`.
# MAGIC
# MAGIC > **Dica:** Mude `n_estimators` e/ou `max_depth` no RandomForest.
# MAGIC > Registre a nova versao e use `client.set_registered_model_alias(model_name, "challenger", version)`.

# COMMAND ----------

# ============================================================
# Treine um challenger e registre:
#
# run_name_v2 = f"producao_decline_challenger_{nome}_{dt.now().strftime('%Y%m%d_%H%M%S')}"
#
# with mlflow.start_run(run_name=run_name_v2) as run_v2:
#     model_v2 = RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42)
#     model_v2.fit(X_train, y_train)
#     predictions_v2 = model_v2.predict(X_test)
#
#     mae_v2 = mean_absolute_error(y_test, predictions_v2)
#     rmse_v2 = np.sqrt(mean_squared_error(y_test, predictions_v2))
#     r2_v2 = r2_score(y_test, predictions_v2)
#
#     mlflow.log_metric("custom_mae", mae_v2)
#     mlflow.log_metric("custom_rmse", rmse_v2)
#     mlflow.log_metric("custom_r2", r2_v2)
#     run_id_v2 = run_v2.info.run_id
#
# result_v2 = mlflow.register_model(f"runs:/{run_id_v2}/model", model_name)
# client.set_registered_model_alias(model_name, "challenger", result_v2.version)
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 6: Comparar Champion vs Challenger
# MAGIC
# MAGIC Compare as metricas lado a lado. A decisao de promover e **manual** —
# MAGIC metricas offline sao apenas parte da avaliacao.
# MAGIC
# MAGIC > Em producao, considere tambem: testes A/B, validacao de dominio,
# MAGIC > performance em edge-cases (pocos novos, paradas, etc.)

# COMMAND ----------

# ============================================================
# Comparacao Champion vs Challenger:
#
# print(f"{'='*60}")
# print(f"  CHAMPION (v{result_v1.version}) vs CHALLENGER (v{result_v2.version})")
# print(f"{'='*60}")
# print(f"  {'Metrica':<12} {'Champion':>12} {'Challenger':>12} {'Melhor':>10}")
# print(f"  {'-'*46}")
# print(f"  {'MAE':<12} {mae:>12,.2f} {mae_v2:>12,.2f} {'Challenger' if mae_v2 < mae else 'Champion':>10}")
# print(f"  {'RMSE':<12} {rmse:>12,.2f} {rmse_v2:>12,.2f} {'Challenger' if rmse_v2 < rmse else 'Champion':>10}")
# print(f"  {'R2':<12} {r2:>12.4f} {r2_v2:>12.4f} {'Challenger' if r2_v2 > r2 else 'Champion':>10}")
# print(f"{'='*60}")
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 7: Promover Challenger (decisao manual)
# MAGIC
# MAGIC Se o Challenger for melhor, promova executando a celula abaixo:

# COMMAND ----------

# Descomente para promover o Challenger a Champion:
# client.set_registered_model_alias(model_name, "champion", result_v2.version)
# print(f"Challenger v{result_v2.version} PROMOVIDO a Champion!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 8: Inferencia pontual com o modelo Champion
# MAGIC
# MAGIC Carregue sempre pelo alias `champion` — a troca de versao e transparente.
# MAGIC
# MAGIC ```python
# MAGIC import mlflow.pyfunc
# MAGIC model_champion = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")
# MAGIC predicoes = model_champion.predict(X_test)
# MAGIC ```

# COMMAND ----------

# ============================================================
# Inferencia pontual:
#
# import mlflow.pyfunc
# import pandas as pd
#
# model_champion = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")
# predicoes = model_champion.predict(X_test)
#
# df_resultado = pd.DataFrame({
#     "real_bbl": y_test.values,
#     "previsto_bbl": predicoes,
#     "erro_bbl": np.abs(y_test.values - predicoes)
# }).sort_values("erro_bbl", ascending=False)
#
# display(spark.createDataFrame(df_resultado))
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 8: Inferencia em batch (Spark DataFrame)
# MAGIC
# MAGIC Para inferencia em larga escala, carregue o modelo como **Spark UDF**
# MAGIC e aplique diretamente sobre um DataFrame distribuido — sem coletar no driver.
# MAGIC
# MAGIC ```python
# MAGIC predict_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_name}@champion", result_type="double")
# MAGIC df_batch = df_features.withColumn("previsao", predict_udf(*[col(c) for c in feature_cols]))
# MAGIC ```

# COMMAND ----------

# ============================================================
# Inferencia batch:
#
# predict_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_name}@champion", result_type="double")
#
# df_batch_inference = (
#     df_features
#     .withColumn("producao_prevista_bbl", predict_udf(*[col(c) for c in feature_cols]))
#     .select("campo", "ano", "semana", "vol_oleo_semanal", "producao_prevista_bbl")
#     .withColumn("erro_bbl", col("vol_oleo_semanal") - col("producao_prevista_bbl"))
# )
#
# df_batch_inference.display()
# ============================================================

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
# MAGIC | Promocao | Mover alias `champion` para melhor versao | MlflowClient aliases |
# MAGIC | Inferencia pontual | Carregar modelo por alias e prever | mlflow.pyfunc.load_model |
# MAGIC | Inferencia batch | Aplicar modelo como UDF sobre Spark DataFrame | mlflow.pyfunc.spark_udf |
# MAGIC
# MAGIC ### Aliases no Unity Catalog
# MAGIC
# MAGIC | Alias | Significado | Como carregar |
# MAGIC |-------|------------|---------------|
# MAGIC | `champion` | Modelo em producao | `models:/{nome}@champion` |
# MAGIC | `challenger` | Candidato a producao | `models:/{nome}@challenger` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Proximo:** Lab 03 (03_Lab_AIBI) para criar Dashboards e Genie!
