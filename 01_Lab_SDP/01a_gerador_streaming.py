# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Databricks - Lab 01: Spark Declarative Pipelines
# MAGIC ## Gerador de Dados Streaming
# MAGIC
# MAGIC **Objetivo:** Simular dados de producao chegando em tempo real do SCADA/PI Historian.
# MAGIC
# MAGIC > **IMPORTANTE:** Execute este notebook em uma **sessao separada** e deixe-o rodando
# MAGIC > enquanto trabalha no pipeline.
# MAGIC >
# MAGIC > Ele gera novos registros de producao a cada 30 segundos, simulando
# MAGIC > a chegada de dados de sensores de pocos offshore.
# MAGIC >
# MAGIC > **Nota:** O `id_producao` segue o formato `{id_poco}_{data}_{sufixo}`,
# MAGIC > consistente com os dados historicos do CSV. Todos os `id_poco` gerados
# MAGIC > referenciam pocos validos (1 a 2000).
# MAGIC
# MAGIC ---

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").collect()[0][0]
nome = usuario.split("@")[0].replace(".", "_").replace("-", "_").lower()
CATALOGO = "workshop_databricks"
SCHEMA = nome
STREAMING_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/producao_streaming/"

print(f"Gerador de streaming: {STREAMING_PATH}")
print(f"Frequencia: novo lote a cada 30 segundos")
print(f"Para parar: clique em Cancel no canto superior direito")

# COMMAND ----------

import random, time, json, string
from datetime import datetime

random.seed()

def gerar_sufixo(n=6):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

lote = 0
while True:
    lote += 1
    registros = []
    n_registros = random.randint(5, 20)

    for _ in range(n_registros):
        id_poco = random.randint(1, 2000)
        data_str = datetime.now().strftime("%Y%m%d")
        registros.append({
            "id_producao": f"{id_poco}_{data_str}_{gerar_sufixo()}",
            "id_poco": id_poco,
            "data_producao": datetime.now().strftime("%Y-%m-%d"),
            "vol_oleo_bbl": round(random.uniform(50, 8000), 2),
            "vol_gas_mm3": round(random.uniform(5, 500), 2),
            "vol_agua_bbl": round(random.uniform(0, 5000), 2),
            "bsw_pct": round(random.uniform(0, 95), 2),
            "pressao_cabeca_psi": round(random.uniform(50, 3000), 1),
            "temperatura_c": round(random.uniform(30, 120), 1),
            "horas_operacao": round(random.uniform(0, 24), 1),
            "tipo_medicao": "Operacional",
            "fonte_dado": "SCADA_Streaming"
        })

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    file_path = f"{STREAMING_PATH}producao_lote_{timestamp}.json"
    content = "\n".join([json.dumps(r) for r in registros])
    dbutils.fs.put(file_path, content, overwrite=True)

    print(f"Lote {lote}: {n_registros} registros -> {file_path}")
    time.sleep(30)
