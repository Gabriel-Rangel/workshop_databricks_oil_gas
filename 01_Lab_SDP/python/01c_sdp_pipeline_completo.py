# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 01: Spark Declarative Pipelines (Python)
# MAGIC ## Pipeline Completo (Solucao de Referencia)
# MAGIC
# MAGIC > Solucao completa de todos os TO-DOs.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = spark.conf.get("schema", "default")
VOLUME_BASE = f"/Volumes/workshop_databricks/{schema}"

PRODUCAO_SCHEMA = StructType([
    StructField("id_producao", StringType()),
    StructField("id_poco", IntegerType()),
    StructField("data_producao", StringType()),
    StructField("vol_oleo_bbl", DoubleType()),
    StructField("vol_gas_mm3", DoubleType()),
    StructField("vol_agua_bbl", DoubleType()),
    StructField("bsw_pct", DoubleType()),
    StructField("pressao_cabeca_psi", DoubleType()),
    StructField("temperatura_c", DoubleType()),
    StructField("horas_operacao", DoubleType()),
    StructField("tipo_medicao", StringType()),
    StructField("fonte_dado", StringType()),
])

# COMMAND ----------

# BRONZE

@dlt.table(comment="Pocos brutos (CSV)")
def bronze_pocos():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{VOLUME_BASE}/pocos_csv/_schema")
        .option("header", "true").option("inferSchema", "true")
        .load(f"{VOLUME_BASE}/pocos_csv/")
        .select("*", current_timestamp().alias("_data_ingestao"),
                col("_metadata.file_name").alias("_arquivo_origem"))
    )

@dlt.table(comment="Producao bruta — CSV + streaming")
def bronze_producao():
    df_csv = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{VOLUME_BASE}/producao_csv/_schema")
        .option("header", "true").option("inferSchema", "true")
        .load(f"{VOLUME_BASE}/producao_csv/"))
    df_streaming = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOLUME_BASE}/producao_streaming/_schema")
        .schema(PRODUCAO_SCHEMA)
        .load(f"{VOLUME_BASE}/producao_streaming/"))
    return (df_csv.unionByName(df_streaming, allowMissingColumns=True)
        .select("*", current_timestamp().alias("_data_ingestao"),
                col("_metadata.file_name").alias("_arquivo_origem")))

# TO-DO 1 RESOLVIDO
@dlt.table(comment="Ordens brutas (JSON)")
def bronze_ordens():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOLUME_BASE}/ordens_json/_schema")
        .option("inferSchema", "true")
        .load(f"{VOLUME_BASE}/ordens_json/")
        .select("*", current_timestamp().alias("_data_ingestao"),
                col("_metadata.file_name").alias("_arquivo_origem"))
    )

# COMMAND ----------

# SILVER

@dlt.table(comment="Pocos validados e deduplicados")
@dlt.expect_or_drop("coordenadas_preenchidas", "latitude IS NOT NULL AND longitude IS NOT NULL")
@dlt.expect_or_drop("profundidade_valida", "profundidade_m > 0")
@dlt.expect_or_drop("data_perfuracao_valida", "data_perfuracao <= current_date()")
@dlt.expect("status_valido", "status IN ('Produzindo', 'Parado', 'Manutencao', 'Abandonado', 'Completacao')")
@dlt.expect_or_fail("id_obrigatorio", "id_poco IS NOT NULL")
def silver_pocos():
    return (
        dlt.read_stream("bronze_pocos")
        .withColumn("id_poco", col("id_poco").cast("int"))
        .withColumn("profundidade_m", col("profundidade_m").cast("double"))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
        .withColumn("data_perfuracao", to_date(col("data_perfuracao"), "yyyy-MM-dd"))
        .withColumn("nome_poco", upper(trim(col("nome_poco"))))
        .withColumn("campo", initcap(trim(col("campo"))))
        .withColumn("status", initcap(trim(col("status"))))
        .withColumn("_processado_em", current_timestamp())
        .dropDuplicates(["codigo_sap"])
    )

# TO-DO 2 RESOLVIDO
@dlt.table(comment="Producao diaria validada")
@dlt.expect_or_drop("volume_oleo_positivo", "vol_oleo_bbl > 0")
@dlt.expect_or_drop("data_producao_valida", "data_producao <= current_date()")
@dlt.expect("bsw_na_faixa", "bsw_pct BETWEEN 0 AND 100")
@dlt.expect("volume_razoavel", "vol_oleo_bbl < 50000")
@dlt.expect_or_fail("id_poco_obrigatorio", "id_poco IS NOT NULL")
def silver_producao():
    return (
        dlt.read_stream("bronze_producao")
        .withColumn("id_producao", col("id_producao").cast("string"))
        .withColumn("id_poco", col("id_poco").cast("int"))
        .withColumn("data_producao", to_date(col("data_producao"), "yyyy-MM-dd"))
        .withColumn("vol_oleo_bbl", col("vol_oleo_bbl").cast("double"))
        .withColumn("vol_gas_mm3", col("vol_gas_mm3").cast("double"))
        .withColumn("vol_agua_bbl", col("vol_agua_bbl").cast("double"))
        .withColumn("bsw_pct", col("bsw_pct").cast("double"))
        .withColumn("pressao_cabeca_psi", col("pressao_cabeca_psi").cast("double"))
        .withColumn("temperatura_c", col("temperatura_c").cast("double"))
        .withColumn("horas_operacao", col("horas_operacao").cast("double"))
        .withColumn("flag_outlier", when(col("vol_oleo_bbl") > 50000, True).otherwise(False))
        .withColumn("flag_bsw_alto", when(col("bsw_pct") > 100, True).otherwise(False))
        .withColumn("ano", year(col("data_producao")))
        .withColumn("mes", month(col("data_producao")))
        .withColumn("dia", dayofmonth(col("data_producao")))
        .withColumn("_processado_em", current_timestamp())
    )

# TO-DO 3 RESOLVIDO
@dlt.table(comment="Ordens validadas")
@dlt.expect_or_drop("custo_positivo", "custo_estimado_brl > 0")
@dlt.expect("status_valido", "status IN ('Aberta', 'Em Execucao', 'Concluida', 'Cancelada', 'Aguardando Material')")
@dlt.expect("prioridade_valida", "prioridade IN ('1-Emergencia', '2-Urgente', '3-Normal', '4-Planejada')")
@dlt.expect("descricao_preenchida", "descricao IS NOT NULL")
@dlt.expect_or_drop("id_poco_preenchido", "id_poco IS NOT NULL")
def silver_ordens():
    return (
        dlt.read_stream("bronze_ordens")
        .withColumn("id_ordem", col("id_ordem").cast("int"))
        .withColumn("id_poco", col("id_poco").cast("int"))
        .withColumn("custo_estimado_brl", col("custo_estimado_brl").cast("double"))
        .withColumn("custo_real_brl", col("custo_real_brl").cast("double"))
        .withColumn("data_abertura", to_date(col("data_abertura"), "yyyy-MM-dd"))
        .withColumn("data_conclusao", to_date(col("data_conclusao"), "yyyy-MM-dd"))
        .withColumn("tipo_manutencao", initcap(trim(col("tipo_manutencao"))))
        .withColumn("status", initcap(trim(col("status"))))
        .withColumn("_processado_em", current_timestamp())
    )

# COMMAND ----------

# GOLD

# TO-DO 4 RESOLVIDO
@dlt.table(comment="Producao por campo")
def gold_producao_por_campo():
    return (
        dlt.read("silver_producao")
        .join(dlt.read("silver_pocos"), "id_poco", "inner")
        .groupBy("campo", "bacia")
        .agg(
            count("id_producao").alias("total_medicoes"),
            countDistinct("id_poco").alias("pocos_ativos"),
            round(sum("vol_oleo_bbl"), 2).alias("producao_total_bbl"),
            round(avg("vol_oleo_bbl"), 2).alias("producao_media_bbl"),
            round(avg("bsw_pct"), 2).alias("bsw_medio_pct"),
            round(avg("pressao_cabeca_psi"), 1).alias("pressao_media_psi"),
            round(avg("horas_operacao"), 1).alias("horas_operacao_media"))
        .orderBy(col("producao_total_bbl").desc())
    )

@dlt.table(comment="Custos de manutencao")
def gold_custos_manutencao():
    return (
        dlt.read("silver_ordens")
        .groupBy("tipo_manutencao", "equipamento")
        .agg(
            count("id_ordem").alias("total_ordens"),
            round(sum("custo_estimado_brl"), 2).alias("custo_estimado_total"),
            round(avg("custo_estimado_brl"), 2).alias("custo_estimado_medio"),
            round(sum("custo_real_brl"), 2).alias("custo_real_total"),
            round(avg("custo_real_brl"), 2).alias("custo_real_medio"),
            countDistinct("id_poco").alias("pocos_afetados"))
        .orderBy(col("custo_estimado_total").desc())
    )

# TO-DO 5 RESOLVIDO
@dlt.table(comment="Eficiencia operacional por campo")
def gold_eficiencia_operacional():
    producao = dlt.read("silver_producao")
    pocos = dlt.read("silver_pocos")
    ordens = dlt.read("silver_ordens")

    prod_campo = (
        producao.join(pocos, "id_poco", "inner")
        .groupBy("campo")
        .agg(
            countDistinct("id_poco").alias("total_pocos"),
            round(sum("vol_oleo_bbl"), 2).alias("producao_total_bbl"),
            round(avg("horas_operacao"), 1).alias("uptime_medio_horas"),
            round(avg("bsw_pct"), 2).alias("bsw_medio_pct"),
            round(sum("vol_oleo_bbl") / countDistinct("id_poco"), 2).alias("producao_media_por_poco"))
    )
    manut_campo = (
        ordens.join(pocos, "id_poco", "inner")
        .groupBy("campo")
        .agg(
            count("id_ordem").alias("total_ordens_manutencao"),
            round(sum("custo_estimado_brl"), 2).alias("custo_manutencao_total"))
    )
    return prod_campo.join(manut_campo, "campo", "left").orderBy(col("producao_total_bbl").desc())
