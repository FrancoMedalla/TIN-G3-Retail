# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparación del entorno
# MAGIC Crea los esquemas `bronce`, `plata`, `oro` y `control`, el Volume `bronce.fuentes` (archivos de origen) y el
# MAGIC Volume `oro.salidas` (cifras en CSV), y copia las fuentes desde la carpeta `data/` del repositorio.
# MAGIC Puede ejecutarse cualquier número de veces.

# COMMAND ----------

import os
import shutil

from comun import CAPAS, CATALOGO, DIR_FUENTES, DIR_SALIDAS, EN_DATABRICKS, RAIZ_REPO, spark

ARCHIVOS = ["origen_pos.sqlite", "origen_maestro_productos.csv", "origen_promociones.json",
            "ipc_2025_respaldo.json"]

if EN_DATABRICKS:
    for capa in CAPAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{capa}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronce.fuentes")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.oro.salidas")
    origen = os.path.join(RAIZ_REPO, "data")
    for archivo in ARCHIVOS:
        destino = os.path.join(DIR_FUENTES, archivo)
        if os.path.exists(os.path.join(origen, archivo)):
            shutil.copyfile(os.path.join(origen, archivo), destino)
            print(f"Copiado {archivo} -> {destino}")
        elif os.path.exists(destino):
            print(f"{archivo} ya está en el Volume")
        else:
            raise FileNotFoundError(f"Falta {archivo} en {DIR_FUENTES}")
else:
    os.makedirs(DIR_SALIDAS, exist_ok=True)
    faltan = [a for a in ARCHIVOS if not os.path.exists(os.path.join(DIR_FUENTES, a))]
    assert not faltan, f"Faltan archivos en {DIR_FUENTES}: {faltan}"
    print("Entorno local listo. Fuentes en", DIR_FUENTES)
