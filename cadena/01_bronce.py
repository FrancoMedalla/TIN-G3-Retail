# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Capa bronce
# MAGIC Carga las cuatro fuentes sin transformarlas (todo como texto) y agrega el origen y la corrida de cada fila.
# MAGIC Carga completa: cada corrida reemplaza las tablas. Si la API del IPC no responde, se usa el respaldo del
# MAGIC Volume y la corrida queda marcada como "respaldo".

# COMMAND ----------

import json
import os
import shutil
import sqlite3
import tempfile
import time

import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from comun import (DIR_FUENTES, ID_CORRIDA, URL_IPC, Pruebas, ahora, escribir, escribir_pandas,
                   leer_json_fuente, spark)

pruebas = Pruebas("bronce")
listas = {}
carga = []


def preparar(df, tabla, fuente, filas_origen):
    df = (df.withColumn("_fuente", F.lit(fuente))
            .withColumn("_id_corrida", F.lit(ID_CORRIDA))
            .withColumn("_fecha_carga", F.lit(ahora().isoformat(timespec="seconds"))))
    filas = df.count()
    listas[tabla] = df
    carga.append({"id_corrida": ID_CORRIDA, "fecha_hora": ahora(), "tabla": tabla, "fuente": fuente,
                  "filas_origen": int(filas_origen), "filas_bronce": int(filas)})
    print(f"{tabla:<22} {filas:>7,} filas")
    return filas

# COMMAND ----------

# MAGIC %md ## Fuente 1 · POS (SQLite)
# MAGIC El archivo se copia a disco local antes de abrirlo, porque SQLite requiere acceso aleatorio.

# COMMAND ----------

ruta_sqlite = os.path.join(tempfile.mkdtemp(), "origen_pos.sqlite")
shutil.copyfile(os.path.join(DIR_FUENTES, "origen_pos.sqlite"), ruta_sqlite)
con = sqlite3.connect(ruta_sqlite)
tablas_pos = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
ESPERADAS_POS = {"boletas", "boleta_detalle", "clientes", "comunas", "medios_pago", "regiones", "tiendas"}
pruebas.verificar("B-POS-01", "POS (SQLite)", "Están las 7 tablas esperadas del POS",
                  ESPERADAS_POS <= set(tablas_pos), len(ESPERADAS_POS - set(tablas_pos)))
for t in tablas_pos:
    cur = con.execute(f'SELECT * FROM "{t}"')
    columnas = [d[0] for d in cur.description]
    filas = [tuple(None if v is None else str(v) for v in fila) for fila in cur.fetchall()]
    esquema = StructType([StructField(c, StringType(), True) for c in columnas])
    n = preparar(spark.createDataFrame(filas, esquema), f"pos_{t}", "origen_pos.sqlite", len(filas))
    pruebas.verificar(f"B-POS-{t}", "POS (SQLite)", f"pos_{t}: mismas filas en bronce que en el origen",
                      n == len(filas), abs(n - len(filas)))
con.close()

# COMMAND ----------

# MAGIC %md ## Fuente 2 · Maestro de productos (CSV `;`, Windows-1252)

# COMMAND ----------

ruta_csv = os.path.join(DIR_FUENTES, "origen_maestro_productos.csv")
productos = (spark.read.option("header", True).option("sep", ";").option("encoding", "iso-8859-1")
             .option("inferSchema", False).csv(ruta_csv))
filas_csv = len(pd.read_csv(ruta_csv, sep=";", encoding="cp1252", dtype=str))
n = preparar(productos, "productos", "origen_maestro_productos.csv", filas_csv)
pruebas.verificar("B-CSV-01", "Maestro (CSV)", "Mismas filas en bronce que en una lectura independiente",
                  n == filas_csv, abs(n - filas_csv))
pruebas.verificar("B-CSV-02", "Maestro (CSV)", "Trae las columnas sku, costo_unitario y precio_lista",
                  {"sku", "costo_unitario", "precio_lista"} <= set(productos.columns))

# COMMAND ----------

# MAGIC %md ## Fuente 3 · Campañas (JSON)

# COMMAND ----------

ruta_json = os.path.join(DIR_FUENTES, "origen_promociones.json")
promo = spark.read.option("multiline", True).json(ruta_json)
campanas = (promo.select("cadena", F.explode("campanas").alias("c"))
            .select("cadena", F.col("c.campana_id").alias("campana_id"), F.col("c.nombre").alias("nombre"),
                    F.col("c.tipo").alias("tipo"), F.col("c.fecha_ini").alias("fecha_ini"),
                    F.col("c.fecha_fin").alias("fecha_fin"),
                    F.col("c.descuento_pct").cast("string").alias("descuento_pct"),
                    F.col("c.canales").alias("canales"), F.size("c.skus").alias("n_skus")))
campana_sku = (promo.select(F.explode("campanas").alias("c"))
               .select(F.col("c.campana_id").alias("campana_id"), F.explode("c.skus").alias("sku")))
p = leer_json_fuente("origen_promociones.json")
n1 = preparar(campanas, "promo_campanas", "origen_promociones.json", len(p["campanas"]))
n2 = preparar(campana_sku, "promo_campana_sku", "origen_promociones.json",
              sum(len(c["skus"]) for c in p["campanas"]))
sin_fechas = campanas.filter(F.col("fecha_ini").isNull() | F.col("fecha_fin").isNull()).count()
pruebas.verificar("B-JSON-01", "Campañas (JSON)", "Mismas campañas y SKU que el archivo",
                  n1 == len(p["campanas"]) and n2 == sum(len(c["skus"]) for c in p["campanas"]))
pruebas.verificar("B-JSON-02", "Campañas (JSON)", "Toda campaña tiene fecha de inicio y de fin", sin_fechas == 0,
                  sin_fechas)

# COMMAND ----------

# MAGIC %md ## Fuente 4 · IPC mensual 2025 (API de mindicador.cl)
# MAGIC Tres intentos. Si la API responde, la respuesta se guarda como respaldo con su fecha de descarga. Si fallan,
# MAGIC se usa ese respaldo, la prueba B-API-02 lo advierte y el tablero muestra la fecha del respaldo.
# MAGIC La falla se puede reproducir ejecutando el Job con el parámetro `url_ipc` apuntando a una URL inválida.

# COMMAND ----------

RESPALDO_IPC = os.path.join(DIR_FUENTES, "ipc_2025_respaldo.json")
payload, origen_ipc, error_api = None, None, ""
for intento in range(1, 4):
    try:
        resp = requests.get(URL_IPC, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        assert payload.get("serie"), "respuesta sin serie"
        origen_ipc = "api"
        break
    except Exception as e:
        error_api = str(e)[:200]
        print(f"Intento {intento} falló: {error_api}")
        time.sleep(2)
if origen_ipc == "api":
    fecha_respaldo = ahora().strftime("%Y-%m-%d")
    with open(RESPALDO_IPC, "w", encoding="utf-8") as f:
        json.dump({"fecha_descarga": fecha_respaldo, **payload}, f, ensure_ascii=False, indent=1)
    print(f"API OK; respaldo actualizado ({fecha_respaldo})")
else:
    if not os.path.exists(RESPALDO_IPC):
        raise RuntimeError("La API no respondió y no hay respaldo del IPC: no se publica la corrida.")
    payload = leer_json_fuente("ipc_2025_respaldo.json")
    origen_ipc = "respaldo"
    fecha_respaldo = payload.get("fecha_descarga", "sin fecha")
    print(f"ATENCIÓN: la API no respondió; se usa el respaldo descargado el {fecha_respaldo}")

filas_ipc = [(s["fecha"], float(s["valor"]), payload.get("unidad_medida"), payload.get("codigo"))
             for s in payload["serie"]]
esquema_ipc = StructType([StructField("fecha", StringType()), StructField("valor", DoubleType()),
                          StructField("unidad_medida", StringType()), StructField("codigo", StringType())])
n = preparar(spark.createDataFrame(filas_ipc, esquema_ipc), "ipc_mensual", f"mindicador.cl ({origen_ipc})",
             len(filas_ipc))
meses = {pd.Timestamp(f).tz_convert("America/Santiago").month for f, *_ in filas_ipc
         if pd.Timestamp(f).tz_convert("America/Santiago").year == 2025}
pruebas.verificar("B-API-01", "IPC (API)", "La serie trae los 12 meses de 2025", len(meses) == 12, 12 - len(meses))
pruebas.verificar("B-API-02", "IPC (API)", "El IPC viene de la API y no del respaldo", origen_ipc == "api",
                  severidad="avisa",
                  accion="Se usa el respaldo del Volume; el pie del tablero indica 'IPC desde respaldo'")

# COMMAND ----------

# MAGIC %md ## Cierre: pruebas y publicación de la capa

# COMMAND ----------

pruebas.cerrar()
for tabla, df in listas.items():
    escribir(df, "bronce", tabla)
estado = pd.DataFrame(carga).assign(origen_ipc=origen_ipc, fecha_respaldo_ipc=fecha_respaldo, error_api=error_api)
escribir_pandas(estado, "control", "bitacora_carga", modo="append")
print(f"Bronce publicado. Corrida {ID_CORRIDA}. IPC desde: {origen_ipc}")
