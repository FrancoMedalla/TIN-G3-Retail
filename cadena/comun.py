"""Configuración, lectura y escritura por capa, pruebas de calidad, bitácora y definición de la métrica.

Ejecuta en Databricks (tablas Delta en Unity Catalog, archivos en Volumes) o en local (Parquet en
lakehouse_local/). La métrica se define una sola vez en METRICAS; la metric view y las cifras publicadas
se generan desde ese diccionario.
"""
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

EN_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
ZONA = "America/Santiago"
RAIZ_REPO = os.environ.get("RAIZ_REPO", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Spark trabaja en UTC: las fechas del POS son días calendario y en America/Santiago algunas medianoches no
# existen por el cambio de horario (por ejemplo, 2025-09-07 00:00). Las conversiones a hora de Chile se hacen
# de forma explícita donde corresponde.
if EN_DATABRICKS:
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
else:
    # Arrow se desactiva en local por incompatibilidad de Spark 3.5 con Java 21.
    spark = (SparkSession.builder.appName("multitienda").master("local[*]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")


def parametro(nombre, defecto):
    """Parámetro del Job en Databricks; variable de entorno en mayúsculas en local."""
    if EN_DATABRICKS:
        try:
            from databricks.sdk.runtime import dbutils
            dbutils.widgets.text(nombre, defecto)
            return dbutils.widgets.get(nombre) or defecto
        except Exception:
            return defecto
    return os.environ.get(nombre.upper(), defecto)


CATALOGO = parametro("catalogo", "workspace")
ID_CORRIDA = parametro("id_corrida", "") or datetime.now(ZoneInfo(ZONA)).strftime("%Y%m%d-%H%M%S")
URL_IPC = parametro("url_ipc", "https://mindicador.cl/api/ipc/2025")
CAPAS = ["bronce", "plata", "oro", "control"]

if EN_DATABRICKS:
    DIR_FUENTES = f"/Volumes/{CATALOGO}/bronce/fuentes"
    DIR_SALIDAS = f"/Volumes/{CATALOGO}/oro/salidas"
else:
    LAKEHOUSE = os.path.join(RAIZ_REPO, "lakehouse_local")
    DIR_FUENTES = os.environ.get("DIR_FUENTES", os.path.join(RAIZ_REPO, "data"))
    DIR_SALIDAS = os.path.join(RAIZ_REPO, "salidas")


def nombre_tabla(capa, tabla):
    return f"{CATALOGO}.{capa}.{tabla}"


def ruta_local(capa, tabla):
    return os.path.join(LAKEHOUSE, capa, tabla)


def escribir(df, capa, tabla, modo="overwrite"):
    """Escribe una tabla. 'overwrite' reconstruye la tabla completa; 'append' solo se usa en control."""
    invalidas = [c for c in df.columns if any(ch in c for ch in " ,;{}()\n\t=")]
    assert not invalidas, f"Nombres de columna no válidos para Delta: {invalidas}"
    if EN_DATABRICKS:
        (df.write.mode(modo).option("overwriteSchema", "true" if modo == "overwrite" else "false")
           .saveAsTable(nombre_tabla(capa, tabla)))
    else:
        df.write.mode(modo).parquet(ruta_local(capa, tabla))
    print(f"  -> {capa}.{tabla} ({modo})")


def leer(capa, tabla):
    if EN_DATABRICKS:
        return spark.table(nombre_tabla(capa, tabla))
    return spark.read.parquet(ruta_local(capa, tabla))


def a_pandas(capa, tabla, sin_metadatos=True):
    df = leer(capa, tabla)
    fechas = [f.name for f in df.schema.fields if isinstance(f.dataType, DateType)]
    pdf = df.toPandas()
    for c in fechas:
        pdf[c] = pd.to_datetime(pdf[c])
    if sin_metadatos:
        pdf = pdf[[c for c in pdf.columns if not c.startswith("_")]]
    return pdf


def desde_pandas(pdf):
    """pandas -> Spark. Las fechas de día completo pasan como DATE y las fechas con hora como texto ISO,
    para que ninguna conversión de zona horaria las modifique."""
    pdf = pdf.copy()
    for c in pdf.columns:
        t = str(pdf[c].dtype)
        if t.startswith("datetime64"):
            s = pdf[c]
            con_hora = (s.dropna() != s.dropna().dt.normalize()).any()
            valores = s.dt.strftime("%Y-%m-%d %H:%M:%S") if con_hora else s.dt.date
            pdf[c] = valores.astype(object).where(s.notna(), None)
        elif t in ("string", "str", "boolean", "object") or t.startswith("Int"):
            pdf[c] = pdf[c].astype(object).where(pdf[c].notna(), None)
    try:
        return spark.createDataFrame(pdf)
    except Exception:
        try:
            anterior = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        except Exception:
            anterior = None
        try:
            return spark.createDataFrame(pdf)
        finally:
            if anterior is not None:
                spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", anterior)


def escribir_pandas(pdf, capa, tabla, modo="overwrite"):
    escribir(desde_pandas(pdf), capa, tabla, modo)


def guardar_salida_csv(pdf, nombre, **kw):
    os.makedirs(DIR_SALIDAS, exist_ok=True)
    ruta = os.path.join(DIR_SALIDAS, nombre)
    pdf.to_csv(ruta, encoding="utf-8-sig", **kw)
    print(f"  -> {ruta}")


def ahora():
    return datetime.now(ZoneInfo(ZONA)).replace(tzinfo=None)


def leer_json_fuente(nombre):
    with open(os.path.join(DIR_FUENTES, nombre), encoding="utf-8") as f:
        return json.load(f)


class Pruebas:
    """Pruebas de calidad de una capa.

    cerrar() registra los resultados en control.pruebas_resultados y lanza un error si falló una prueba
    bloqueante. Cada capa escribe sus tablas después de cerrar(), de modo que una falla bloqueante impide
    publicarla.
    """

    def __init__(self, capa):
        self.capa = capa
        self.filas = []

    def verificar(self, id_prueba, fuente, que_verifica, ok, filas_falla=0, severidad="bloquea", accion=None):
        ok = bool(ok)
        if accion is None:
            accion = ("Detener la cadena: no se publica la capa ni se refresca el tablero"
                      if severidad == "bloquea" else "Continuar y advertir en el pie del tablero")
        self.filas.append({
            "id_corrida": ID_CORRIDA, "fecha_hora": ahora(), "capa": self.capa, "id_prueba": id_prueba,
            "fuente_o_metrica": fuente, "que_verifica": que_verifica, "severidad": severidad,
            "resultado": "pasó" if ok else "falló", "filas_que_fallan": int(filas_falla),
            "accion_si_falla": accion})
        marca = "OK   " if ok else ("FALLA" if severidad == "bloquea" else "AVISO")
        print(f"  [{marca}] {id_prueba} {que_verifica} ({int(filas_falla)} filas)")

    def cerrar(self):
        df = pd.DataFrame(self.filas)
        escribir_pandas(df, "control", "pruebas_resultados", modo="append")
        fallas = df[(df.resultado == "falló") & (df.severidad == "bloquea")]
        if len(fallas):
            raise RuntimeError(f"Fallaron {len(fallas)} pruebas bloqueantes en {self.capa}: "
                               f"{', '.join(fallas.id_prueba)}. La capa no se publica.")
        print(f"Pruebas de {self.capa}: {len(df)} ejecutadas, {int((df.resultado == 'pasó').sum())} aprobadas.")


def _responsables():
    ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "responsables_bitacora.csv")
    if not os.path.exists(ruta):
        return {}
    r = pd.read_csv(ruta, dtype=str).fillna("")
    return {f.id: (f.decidio, f.fecha) for f in r.itertuples()}


RESPONSABLES = _responsables()


class Bitacora:
    """Bitácora de supuestos de una capa; el responsable y la fecha salen de responsables_bitacora.csv."""

    def __init__(self, capa):
        self.capa = capa
        self.filas = []

    def registrar(self, codigo, tabla, regla, justificacion, filas, si_fuera_falso, origen):
        quien, cuando = RESPONSABLES.get(codigo, ("Sin asignar", "Sin fecha"))
        self.filas.append({
            "id": codigo, "tabla": tabla, "regla": regla, "justificacion": justificacion,
            "origen_evidencia": origen, "filas_afectadas": int(filas),
            "si_fuera_falso": si_fuera_falso, "decidio": quien, "fecha": cuando})
        print(f"[{codigo}] {regla}: {int(filas):,} filas")

    def guardar(self):
        escribir_pandas(pd.DataFrame(self.filas), "control", f"bitacora_{self.capa}")


# Definición de la métrica. Fuente: oro.cliente_ventana, una fila por cliente y ventana con indicadores 0/1.
CORTE_1, CORTE_2 = "2025-06-30", "2025-12-31"
VENTANAS = [30, 90, 180]
VENTANA_BASE = 90
UMBRAL_LECTURA = 0.5

METRICAS = {
    "tasa_retencion": ("SUM(retenido) / SUM(activo_jun)",
                       "Tasa de retención semestral: activos al 30-jun que siguen activos al 31-dic / activos al 30-jun"),
    "tasa_reposicion": ("SUM(ganado) / SUM(activo_dic)",
                        "Reposición: activos al 31-dic que no estaban activos al 30-jun / activos al 31-dic"),
    "activos_jun": ("SUM(activo_jun)", "Clientes activos al 30-jun"),
    "retenidos": ("SUM(retenido)", "Activos al 30-jun que siguen activos al 31-dic"),
    "perdidos": ("SUM(perdido)", "Activos al 30-jun que no están activos al 31-dic"),
    "ganados": ("SUM(ganado)", "Activos al 31-dic que no estaban activos al 30-jun"),
    "ganados_nuevos": ("SUM(ganado_nuevo)", "Ganados con alta en 2025 y primera visita después del 30-jun"),
    "ganados_reactivados": ("SUM(ganado_reactivado)", "Ganados que no son nuevos"),
    "activos_dic": ("SUM(activo_dic)", "Clientes activos al 31-dic"),
}
DIMENSIONES = {
    "ventana_dias": "Ventana de actividad W (30, 90 o 180 días)",
    "tipo_cliente": "Base heredada (alta antes de 2025) o Nuevo 2025",
    "segmento": "Clásico, Plus o Black (estado actual)",
    "tienda_principal": "Local con más visitas del cliente en la ventana de junio (solo activos al 30-jun)",
    "alta_posterior_a_primera_compra": "1 si la fecha de alta es posterior a la primera visita de 2025",
}
REGLA_LECTURA = (f"La cadena REPONE si más del {UMBRAL_LECTURA:.0%} de los activos al 31-dic no estaban activos "
                 "al 30-jun; si no, RETIENE. Regla fijada antes de calcular.")

# Cifras publicadas en el informe; la prueba S-MET-05 advierte si la cadena deja de reproducirlas.
CIFRAS_PUBLICADAS = {30: (0.1860, 0.8609), 90: (0.4148, 0.6944), 180: (0.5864, 0.5870)}


def sql_metricas(tabla_fuente, dimensiones=("ventana_dias",), filtro=None):
    """Consulta equivalente a la metric view, generada desde METRICAS."""
    dims = ", ".join(dimensiones)
    medidas = ",\n  ".join(f"{expr} AS {nombre}" for nombre, (expr, _) in METRICAS.items())
    donde = f"WHERE {filtro}" if filtro else ""
    return f"SELECT {dims},\n  {medidas}\nFROM {tabla_fuente} {donde}\nGROUP BY {dims}\nORDER BY {dims}"


def yaml_metric_view(tabla_fuente):
    lineas = ["version: 1.1",
              'comment: "Retención y reposición de clientes de Multitienda del Sur."',
              f"source: {tabla_fuente}", "dimensions:"]
    for nombre, comentario in DIMENSIONES.items():
        lineas += [f"  - name: {nombre}", f"    expr: {nombre}", f'    comment: "{comentario}"']
    lineas.append("measures:")
    for nombre, (expr, comentario) in METRICAS.items():
        lineas += [f"  - name: {nombre}", f"    expr: {expr}", f'    comment: "{comentario}"']
    return "\n".join(lineas)
