-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Workshop Databricks - Lab 01: Spark Declarative Pipelines (SQL)
-- MAGIC ## Pipeline Completo (Solucao de Referencia)
-- MAGIC
-- MAGIC > Solucao completa de todos os TO-DOs do Lab 01.
-- MAGIC > Catalog/Schema padrao configurados no pipeline settings.

-- COMMAND ----------

-- BRONZE
CREATE OR REFRESH STREAMING TABLE bronze_pocos
COMMENT 'Pocos brutos (CSV)'
AS SELECT *, current_timestamp() AS _data_ingestao, _metadata.file_name AS _arquivo_origem
FROM STREAM read_files('/Volumes/workshop_databricks/${schema}/pocos_csv/', format => 'csv', header => true, inferSchema => true);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_producao
COMMENT 'Producao bruta — CSV historico + streaming SCADA'
AS
  SELECT
    CAST(id_producao AS STRING) AS id_producao, CAST(id_poco AS INT) AS id_poco,
    CAST(data_producao AS STRING) AS data_producao, CAST(vol_oleo_bbl AS DOUBLE) AS vol_oleo_bbl,
    CAST(vol_gas_mm3 AS DOUBLE) AS vol_gas_mm3, CAST(vol_agua_bbl AS DOUBLE) AS vol_agua_bbl,
    CAST(bsw_pct AS DOUBLE) AS bsw_pct, CAST(pressao_cabeca_psi AS DOUBLE) AS pressao_cabeca_psi,
    CAST(temperatura_c AS DOUBLE) AS temperatura_c, CAST(horas_operacao AS DOUBLE) AS horas_operacao,
    CAST(tipo_medicao AS STRING) AS tipo_medicao, CAST(fonte_dado AS STRING) AS fonte_dado,
    current_timestamp() AS _data_ingestao, _metadata.file_name AS _arquivo_origem
  FROM STREAM read_files('/Volumes/workshop_databricks/${schema}/producao_csv/', format => 'csv', header => true, inferSchema => true)
  UNION ALL
  SELECT
    id_producao, id_poco, data_producao, vol_oleo_bbl, vol_gas_mm3, vol_agua_bbl,
    bsw_pct, pressao_cabeca_psi, temperatura_c, horas_operacao, tipo_medicao, fonte_dado,
    current_timestamp() AS _data_ingestao, _metadata.file_name AS _arquivo_origem
  FROM STREAM read_files('/Volumes/workshop_databricks/${schema}/producao_streaming/', format => 'json',
    schema => 'id_producao STRING, id_poco INT, data_producao STRING, vol_oleo_bbl DOUBLE, vol_gas_mm3 DOUBLE, vol_agua_bbl DOUBLE, bsw_pct DOUBLE, pressao_cabeca_psi DOUBLE, temperatura_c DOUBLE, horas_operacao DOUBLE, tipo_medicao STRING, fonte_dado STRING');

-- COMMAND ----------

-- TO-DO 1 RESOLVIDO
CREATE OR REFRESH STREAMING TABLE bronze_ordens
COMMENT 'Ordens de manutencao brutas (JSON)'
AS SELECT *, current_timestamp() AS _data_ingestao, _metadata.file_name AS _arquivo_origem
FROM STREAM read_files('/Volumes/workshop_databricks/${schema}/ordens_json/', format => 'json', inferSchema => true);

-- COMMAND ----------

-- SILVER - POCOS
CREATE OR REFRESH STREAMING TABLE silver_pocos (
  CONSTRAINT coordenadas_preenchidas  EXPECT (latitude IS NOT NULL AND longitude IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT profundidade_valida      EXPECT (profundidade_m > 0)                             ON VIOLATION DROP ROW,
  CONSTRAINT data_perfuracao_valida   EXPECT (data_perfuracao <= current_date())               ON VIOLATION DROP ROW,
  CONSTRAINT status_valido            EXPECT (status IN ('Produzindo', 'Parado', 'Manutencao', 'Abandonado', 'Completacao')),
  CONSTRAINT id_obrigatorio           EXPECT (id_poco IS NOT NULL)                             ON VIOLATION FAIL UPDATE
)
COMMENT 'Pocos validados com regras de qualidade'
AS SELECT
  CAST(id_poco AS INT) AS id_poco, codigo_sap, UPPER(TRIM(nome_poco)) AS nome_poco,
  INITCAP(TRIM(campo)) AS campo, bacia, tipo_poco, metodo_elevacao,
  CAST(profundidade_m AS DOUBLE) AS profundidade_m,
  CAST(latitude AS DOUBLE) AS latitude, CAST(longitude AS DOUBLE) AS longitude,
  INITCAP(TRIM(status)) AS status,
  TO_DATE(data_perfuracao, 'yyyy-MM-dd') AS data_perfuracao, operador,
  current_timestamp() AS _processado_em
FROM STREAM(LIVE.bronze_pocos);

-- COMMAND ----------

-- TO-DO 2 RESOLVIDO: colunas ano, mes, dia
CREATE OR REFRESH STREAMING TABLE silver_producao (
  CONSTRAINT volume_oleo_positivo     EXPECT (vol_oleo_bbl > 0)                ON VIOLATION DROP ROW,
  CONSTRAINT data_producao_valida     EXPECT (data_producao <= current_date()) ON VIOLATION DROP ROW,
  CONSTRAINT bsw_na_faixa            EXPECT (bsw_pct BETWEEN 0 AND 100),
  CONSTRAINT volume_razoavel          EXPECT (vol_oleo_bbl < 50000),
  CONSTRAINT id_poco_obrigatorio      EXPECT (id_poco IS NOT NULL)             ON VIOLATION FAIL UPDATE
)
COMMENT 'Producao diaria validada'
AS SELECT
  CAST(id_producao AS STRING) AS id_producao,
  CAST(id_poco AS INT) AS id_poco,
  TO_DATE(data_producao, 'yyyy-MM-dd') AS data_producao,
  CAST(vol_oleo_bbl AS DOUBLE) AS vol_oleo_bbl,
  CAST(vol_gas_mm3 AS DOUBLE) AS vol_gas_mm3,
  CAST(vol_agua_bbl AS DOUBLE) AS vol_agua_bbl,
  CAST(bsw_pct AS DOUBLE) AS bsw_pct,
  CAST(pressao_cabeca_psi AS DOUBLE) AS pressao_cabeca_psi,
  CAST(temperatura_c AS DOUBLE) AS temperatura_c,
  CAST(horas_operacao AS DOUBLE) AS horas_operacao,
  tipo_medicao, fonte_dado,
  CASE WHEN CAST(vol_oleo_bbl AS DOUBLE) > 50000 THEN TRUE ELSE FALSE END AS flag_outlier,
  CASE WHEN CAST(bsw_pct AS DOUBLE) > 100 THEN TRUE ELSE FALSE END AS flag_bsw_alto,
  YEAR(TO_DATE(data_producao, 'yyyy-MM-dd'))  AS ano,
  MONTH(TO_DATE(data_producao, 'yyyy-MM-dd')) AS mes,
  DAY(TO_DATE(data_producao, 'yyyy-MM-dd'))   AS dia,
  current_timestamp() AS _processado_em
FROM STREAM(LIVE.bronze_producao);

-- COMMAND ----------

-- TO-DO 3 RESOLVIDO: constraint id_poco_preenchido
CREATE OR REFRESH STREAMING TABLE silver_ordens (
  CONSTRAINT custo_positivo       EXPECT (custo_estimado_brl > 0)                                                              ON VIOLATION DROP ROW,
  CONSTRAINT status_valido        EXPECT (status IN ('Aberta', 'Em Execucao', 'Concluida', 'Cancelada', 'Aguardando Material')),
  CONSTRAINT prioridade_valida    EXPECT (prioridade IN ('1-Emergencia', '2-Urgente', '3-Normal', '4-Planejada')),
  CONSTRAINT descricao_preenchida EXPECT (descricao IS NOT NULL),
  CONSTRAINT id_poco_preenchido   EXPECT (id_poco IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Ordens de manutencao validadas'
AS SELECT
  CAST(id_ordem AS INT) AS id_ordem, numero_om_sap,
  CAST(id_poco AS INT) AS id_poco,
  INITCAP(TRIM(tipo_manutencao)) AS tipo_manutencao, equipamento, prioridade,
  INITCAP(TRIM(status)) AS status, descricao,
  TO_DATE(data_abertura, 'yyyy-MM-dd') AS data_abertura,
  TO_DATE(data_conclusao, 'yyyy-MM-dd') AS data_conclusao,
  CAST(custo_estimado_brl AS DOUBLE) AS custo_estimado_brl,
  CAST(custo_real_brl AS DOUBLE) AS custo_real_brl,
  centro_custo_sap, responsavel,
  current_timestamp() AS _processado_em
FROM STREAM(LIVE.bronze_ordens);

-- COMMAND ----------

-- GOLD
CREATE OR REFRESH MATERIALIZED VIEW gold_producao_por_campo
COMMENT 'Producao por campo'
AS SELECT
  p.campo, p.bacia,
  COUNT(pr.id_producao) AS total_medicoes,
  COUNT(DISTINCT pr.id_poco) AS pocos_ativos,
  ROUND(SUM(pr.vol_oleo_bbl), 2) AS producao_total_bbl,
  ROUND(AVG(pr.vol_oleo_bbl), 2) AS producao_media_bbl,
  ROUND(AVG(pr.bsw_pct), 2) AS bsw_medio_pct,
  ROUND(AVG(pr.pressao_cabeca_psi), 1) AS pressao_media_psi,
  ROUND(AVG(pr.horas_operacao), 1) AS horas_operacao_media
FROM LIVE.silver_producao pr
INNER JOIN LIVE.silver_pocos p ON pr.id_poco = p.id_poco
GROUP BY p.campo, p.bacia
ORDER BY producao_total_bbl DESC;

-- COMMAND ----------

-- TO-DO 4 RESOLVIDO
CREATE OR REFRESH MATERIALIZED VIEW gold_custos_manutencao
COMMENT 'Custos de manutencao por tipo e equipamento'
AS SELECT
  tipo_manutencao, equipamento,
  COUNT(id_ordem) AS total_ordens,
  ROUND(SUM(custo_estimado_brl), 2) AS custo_estimado_total,
  ROUND(AVG(custo_estimado_brl), 2) AS custo_estimado_medio,
  ROUND(SUM(custo_real_brl), 2) AS custo_real_total,
  ROUND(AVG(custo_real_brl), 2) AS custo_real_medio,
  COUNT(DISTINCT id_poco) AS pocos_afetados
FROM LIVE.silver_ordens
GROUP BY tipo_manutencao, equipamento
ORDER BY custo_estimado_total DESC;

-- COMMAND ----------

-- TO-DO 5 RESOLVIDO
CREATE OR REFRESH MATERIALIZED VIEW gold_eficiencia_operacional
COMMENT 'Eficiencia operacional por campo'
AS
WITH prod_campo AS (
  SELECT p.campo,
    COUNT(DISTINCT pr.id_poco) AS total_pocos,
    ROUND(SUM(pr.vol_oleo_bbl), 2) AS producao_total_bbl,
    ROUND(AVG(pr.horas_operacao), 1) AS uptime_medio_horas,
    ROUND(AVG(pr.bsw_pct), 2) AS bsw_medio_pct,
    ROUND(SUM(pr.vol_oleo_bbl) / COUNT(DISTINCT pr.id_poco), 2) AS producao_media_por_poco
  FROM LIVE.silver_producao pr
  INNER JOIN LIVE.silver_pocos p ON pr.id_poco = p.id_poco
  GROUP BY p.campo
),
manut_campo AS (
  SELECT p.campo,
    COUNT(o.id_ordem) AS total_ordens_manutencao,
    ROUND(SUM(o.custo_estimado_brl), 2) AS custo_manutencao_total
  FROM LIVE.silver_ordens o
  INNER JOIN LIVE.silver_pocos p ON o.id_poco = p.id_poco
  GROUP BY p.campo
)
SELECT pc.*, mc.total_ordens_manutencao, mc.custo_manutencao_total
FROM prod_campo pc
LEFT JOIN manut_campo mc ON pc.campo = mc.campo
ORDER BY pc.producao_total_bbl DESC;
