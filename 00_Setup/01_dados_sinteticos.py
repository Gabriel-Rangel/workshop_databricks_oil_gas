# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Hands-On Databricks - Databricks para Oil & Gas
# MAGIC ## Notebook 01 - Geracao dos Dados Sinteticos
# MAGIC
# MAGIC **Objetivo:** Gerar dados sinteticos simulando o ambiente SAP HANA da operadora
# MAGIC e salva-los nos **Volumes** (landing zone). O **pipeline SDP** sera responsavel
# MAGIC por criar todas as tabelas (Bronze, Silver, Gold).
# MAGIC
# MAGIC | Volume | Formato | Registros | Simula |
# MAGIC |--------|---------|-----------|--------|
# MAGIC | `pocos_csv` | CSV | ~2.000 | SAP PM — cadastro de pocos |
# MAGIC | `producao_csv` | CSV | ~15.000 | SAP PP — producao historica |
# MAGIC | `ordens_json` | JSON | ~5.000 | SAP PM + FI/CO — ordens de manutencao |
# MAGIC | `producao_streaming` | JSON | (vazio) | SCADA — populado pelo gerador no Lab 01 |
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
VOLUME_BASE = f"/Volumes/{CATALOGO}/{SCHEMA}"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Ambiente: {CATALOGO}.{SCHEMA}")
print(f"Volumes:  {VOLUME_BASE}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 1: Gerar dados de pocos e salvar como CSV
# MAGIC
# MAGIC Cadastro de pocos offshore com problemas do SAP PM:
# MAGIC codigos duplicados, coordenadas nulas, status inconsistentes, profundidades negativas.

# COMMAND ----------

import random, string
from datetime import datetime, timedelta
from pyspark.sql import Row

random.seed(42)

campos = ["Frade", "Tubarao Martelo", "Polvo", "Albacora Leste", "Wahoo",
          "Peregrino", "Marlim", "Roncador", "Jubarte", "Buzios"]

bacias_por_campo = {
    "Frade": "Campos", "Tubarao Martelo": "Campos", "Polvo": "Campos",
    "Albacora Leste": "Campos", "Marlim": "Campos", "Roncador": "Campos",
    "Wahoo": "Santos", "Buzios": "Santos", "Jubarte": "Espirito Santo",
    "Peregrino": "Campos"
}

status_validos = ["Produzindo", "Parado", "Manutencao", "Abandonado", "Completacao"]
status_invalidos = ["produzindo", "ATIVO", "parado temp", "", "NULL", "Em Teste"]
tipos_poco = ["Produtor", "Injetor de Agua", "Injetor de Gas", "Observacao", "Pioneiro"]
metodos_elevacao = ["BCS", "Gas Lift", "Plunger Lift", "Natural Flow", "Rod Pump"]

def gerar_codigo_sap():
    return f"POCO-{random.randint(1000,9999)}-{random.choice(['FR','TM','PL','AL','WH'])}"

def gerar_sufixo(n=6):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

pocos_data = []

for i in range(1, 2001):
    campo = random.choice(campos)
    bacia = bacias_por_campo[campo]

    codigo_sap = gerar_codigo_sap()

    if random.random() < 0.03:
        latitude, longitude = None, None
    else:
        base_lat = -22.5 if bacia == "Campos" else -25.0 if bacia == "Santos" else -20.0
        base_lon = -40.0 if bacia == "Campos" else -43.0 if bacia == "Santos" else -38.0
        latitude = round(base_lat + random.uniform(-1, 1), 6)
        longitude = round(base_lon + random.uniform(-1, 1), 6)

    status = random.choice(status_invalidos) if random.random() < 0.05 else random.choice(status_validos)
    profundidade = round(random.uniform(-100, -10), 1) if random.random() < 0.02 else round(random.uniform(1500, 5500), 1)
    data_perfuracao = datetime(2027, 1, 1) + timedelta(days=random.randint(0, 365)) if random.random() < 0.02 else datetime(2005, 1, 1) + timedelta(days=random.randint(0, 7000))

    pocos_data.append(Row(
        id_poco=i, codigo_sap=codigo_sap,
        nome_poco=f"{campo}-{random.choice(tipos_poco)[:3].upper()}-{random.randint(1,50):03d}",
        campo=campo, bacia=bacia, tipo_poco=random.choice(tipos_poco),
        metodo_elevacao=random.choice(metodos_elevacao), profundidade_m=profundidade,
        latitude=latitude, longitude=longitude, status=status,
        data_perfuracao=data_perfuracao.strftime("%Y-%m-%d"), operador="Operadora O&G",
        data_cadastro_sap=(datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2500))).strftime("%Y-%m-%d %H:%M:%S")
    ))

df_pocos = spark.createDataFrame(pocos_data)
df_pocos.write.mode("overwrite").option("header", True).csv(f"{VOLUME_BASE}/pocos_csv/")

print(f"Pocos: {df_pocos.count()} registros -> {VOLUME_BASE}/pocos_csv/")
df_pocos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 2: Gerar dados de producao historica e salvar como CSV
# MAGIC
# MAGIC Dados de producao diaria do SAP PP:
# MAGIC volumes negativos, BSW fora da faixa, outliers, datas futuras.

# COMMAND ----------

producao_data = []
for i in range(1, 15001):
    id_poco = random.randint(1, 2000)
    data_producao = datetime(2027, 1, 1) + timedelta(days=random.randint(0, 365)) if random.random() < 0.02 else datetime(2022, 1, 1) + timedelta(days=random.randint(0, 1100))

    if random.random() < 0.03:
        vol_oleo_bbl = round(random.uniform(-500, -10), 2)
    elif random.random() < 0.02:
        vol_oleo_bbl = round(random.uniform(50000, 200000), 2)
    else:
        vol_oleo_bbl = round(random.uniform(50, 8000), 2)

    id_producao = f"{id_poco}_{data_producao.strftime('%Y%m%d')}_{gerar_sufixo()}"

    producao_data.append(Row(
        id_producao=id_producao, id_poco=id_poco, data_producao=data_producao.strftime("%Y-%m-%d"),
        vol_oleo_bbl=vol_oleo_bbl,
        vol_gas_mm3=round(abs(vol_oleo_bbl) * random.uniform(0.05, 0.3), 2),
        vol_agua_bbl=round(random.uniform(0, abs(vol_oleo_bbl) * 2), 2),
        bsw_pct=round(random.uniform(100, 150), 2) if random.random() < 0.04 else round(random.uniform(0, 95), 2),
        pressao_cabeca_psi=round(random.uniform(50, 3000), 1),
        temperatura_c=round(random.uniform(30, 120), 1),
        horas_operacao=round(random.uniform(0, 24), 1),
        tipo_medicao=random.choice(["Fiscal", "Operacional", "Teste"]),
        fonte_dado=random.choice(["SAP_PP", "PI_Historian", "SCADA", "Manual"])
    ))

df_producao = spark.createDataFrame(producao_data)
df_producao.write.mode("overwrite").option("header", True).csv(f"{VOLUME_BASE}/producao_csv/")

print(f"Producao: {df_producao.count()} registros -> {VOLUME_BASE}/producao_csv/")
df_producao.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 3: Gerar dados de ordens de manutencao e salvar como JSON
# MAGIC
# MAGIC Ordens do SAP PM: equipamentos orfaos, custos negativos, descricoes nulas.

# COMMAND ----------

tipos_manutencao = ["Preventiva", "Corretiva", "Preditiva", "Emergencial", "Parada Programada"]
equipamentos = ["BCS (Bomba Centrifuga Submersa)", "Arvore de Natal Molhada", "Manifold",
                "Riser Flexivel", "Umbilical", "FPSO - Turbocompressor",
                "FPSO - Separador", "FPSO - Bomba de Exportacao",
                "Cabeca de Poco", "Valvula de Seguranca (DHSV)"]
status_om_validos = ["Aberta", "Em Execucao", "Concluida", "Cancelada", "Aguardando Material"]
status_om_invalidos = ["aberta", "CONC", "pendente", "", "Em Aprovacao"]
prioridades_validas = ["1-Emergencia", "2-Urgente", "3-Normal", "4-Planejada"]
prioridades_invalidas = ["Alta", "5", "URGENTE", "", "Baixa"]
descricoes = [
    "Substituicao de BCS por queda de performance",
    "Reparo em valvula de seguranca DHSV",
    "Inspecao de riser flexivel - anomalia detectada por ROV",
    "Troca de selos do turbocompressor FPSO",
    "Manutencao preventiva arvore de natal molhada",
    "Calibracao de sensores de pressao e temperatura",
    "Reparo em umbilical - perda de sinal",
    "Limpeza de manifold submarino",
    "Troca de bomba de exportacao FPSO",
    "Inspecao estrutural da plataforma"
]

ordens_data = []
for i in range(1, 5001):
    id_poco = random.randint(5000, 10000) if random.random() < 0.04 else random.randint(1, 2000)
    status = random.choice(status_om_invalidos) if random.random() < 0.05 else random.choice(status_om_validos)
    prioridade = random.choice(prioridades_invalidas) if random.random() < 0.05 else random.choice(prioridades_validas)
    data_abertura = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 1100))
    custo_estimado = round(random.uniform(-50000, -1000), 2) if random.random() < 0.03 else round(random.uniform(5000, 2000000), 2)

    if random.random() < 0.03:
        data_conclusao = (data_abertura - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
    elif status == "Concluida":
        data_conclusao = (data_abertura + timedelta(days=random.randint(1, 90))).strftime("%Y-%m-%d")
    else:
        data_conclusao = None

    ordens_data.append(Row(
        id_ordem=i, numero_om_sap=f"OM-{random.randint(100000, 999999)}",
        id_poco=id_poco, tipo_manutencao=random.choice(tipos_manutencao),
        equipamento=random.choice(equipamentos), prioridade=prioridade, status=status,
        descricao=random.choice(descricoes) if random.random() > 0.06 else None,
        data_abertura=data_abertura.strftime("%Y-%m-%d"), data_conclusao=data_conclusao,
        custo_estimado_brl=custo_estimado,
        custo_real_brl=round(custo_estimado * random.uniform(0.7, 1.5), 2) if custo_estimado > 0 else None,
        centro_custo_sap=f"CC-{random.choice(['FRADE','TMART','POLVO','ALBAC','WAHOO'])}-{random.randint(100,999)}",
        responsavel=random.choice(["Equipe Subsea", "Equipe Topside", "Equipe Eletrica",
                                    "Equipe Mecanica", "Equipe Instrumentacao", "Vendor Externo"])
    ))

df_ordens = spark.createDataFrame(ordens_data)
df_ordens.write.mode("overwrite").json(f"{VOLUME_BASE}/ordens_json/")

print(f"Ordens: {df_ordens.count()} registros -> {VOLUME_BASE}/ordens_json/")
df_ordens.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo
# MAGIC
# MAGIC | Volume | Formato | Registros | Status |
# MAGIC |--------|---------|-----------|--------|
# MAGIC | `pocos_csv` | CSV | ~2.000 | Populado |
# MAGIC | `producao_csv` | CSV | ~15.000 | Populado |
# MAGIC | `ordens_json` | JSON | ~5.000 | Populado |
# MAGIC | `producao_streaming` | JSON | 0 | Vazio (populado pelo gerador no Lab 01) |
# MAGIC
# MAGIC > **Nenhuma tabela foi criada.** O pipeline SDP no Lab 01 e quem cria
# MAGIC > todas as tabelas (Bronze, Silver, Gold) a partir dos arquivos nos Volumes.
# MAGIC
# MAGIC ---
# MAGIC **Pronto!** Va para o **Lab 01 (01_Lab_SDP)**:
# MAGIC 1. Crie o pipeline com `01b_sdp_pipeline_to_do.sql`
# MAGIC 2. Observe os dados de pocos, producao historica e ordens sendo ingeridos
# MAGIC 3. Execute `01a_gerador_streaming` em outra sessao
# MAGIC 4. Veja novos dados de producao chegando via streaming (Auto Loader incremental)
