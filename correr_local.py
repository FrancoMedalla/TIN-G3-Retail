"""Ejecuta la cadena completa en local, en el mismo orden que el Job de Databricks.

    python correr_local.py
    URL_IPC=https://url-invalida python correr_local.py    # reproduce la falla de la API
"""
import os
import runpy
import sys
import time
from datetime import datetime

RAIZ = os.path.dirname(os.path.abspath(__file__))
CADENA = os.path.join(RAIZ, "cadena")
os.environ.setdefault("ID_CORRIDA", datetime.now().strftime("%Y%m%d-%H%M%S") + "-local")
os.environ.setdefault("RAIZ_REPO", RAIZ)
sys.path.insert(0, CADENA)

inicio = time.time()
for nb in ["00_preparar_entorno", "01_bronce", "02_plata", "03_oro", "04_semantica"]:
    print(f"\n========== {nb} ==========")
    runpy.run_path(os.path.join(CADENA, f"{nb}.py"), run_name="__main__")
print(f"\nCadena completa en {time.time() - inicio:.0f} s. Corrida {os.environ['ID_CORRIDA']}.")
