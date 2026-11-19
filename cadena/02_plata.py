# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Capa plata
# MAGIC Limpieza y tipos con 25 reglas (P, C, I, T, CL, B y D), cada una registrada en `control.bitacora_plata`.
# MAGIC Se usa pandas porque el volumen (unas 15 mil líneas) no requiere procesamiento distribuido. Las tablas se
# MAGIC publican solo si pasan las pruebas bloqueantes.

# COMMAND ----------

import re
import unicodedata

import pandas as pd

from comun import Bitacora, Pruebas, a_pandas, escribir_pandas

bit = Bitacora("plata")
registrar = bit.registrar
pruebas = Pruebas("plata")
salida = {}


def leer_raw(tabla):
    return a_pandas("bronce", tabla)


def a_numero(serie):
    # Punto = separador de miles, coma = decimal: '1.223,00' -> 1223; '$1.213' -> 1213; 'N/A' -> NaN.
    s = serie.astype("string").str.strip().str.replace(r"[$\s]", "", regex=True)
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


MESES = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
         "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12}


def parsear_fecha(txt):
    # Formatos del POS: aaaa-mm-dd, dd/mm/aaaa, dd-mm-aa, d-mmm-aaaa y dd-mmm-aaaa.
    if txt is None or pd.isna(txt):
        return pd.NaT
    s = str(txt).strip().lower()
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        a, me, d = m.groups()
    else:
        m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
        if m:
            d, me, a = m.groups()
        else:
            m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{2})", s)
            if m:
                d, me, a = m.groups()
                a = "20" + a
            else:
                m = re.fullmatch(r"(\d{1,2})-([a-z]{3})-(\d{4})", s)
                if not m:
                    return pd.NaT
                d, me, a = m.groups()
                me = MESES.get(me)
    try:
        return pd.Timestamp(int(a), int(me), int(d))
    except (TypeError, ValueError):
        return pd.NaT


def sin_tilde(s):
    return unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower().strip()

# COMMAND ----------

# MAGIC %md ## Maestro de productos (P01 a P04)

# COMMAND ----------

prod = leer_raw("productos")
for c in prod.columns:
    prod[c] = prod[c].astype("string").str.strip()
es_sku = prod.sku.str.fullmatch(r"SKU-\d+", na=False)
registrar("P01", "productos", "Se eliminan filas que no son productos (encabezado repetido y fila TOTAL)",
          "No tienen un SKU válido; son artefactos de la planilla", (~es_sku).sum(),
          "Ninguno: no corresponden a productos", "medido")
prod = prod[es_sku].copy()

MAPA_CATEGORIA = {"abarr": "Abarrotes", "abarrotes": "Abarrotes", "aseo": "Limpieza", "limpieza": "Limpieza",
                  "beb": "Bebidas", "bebidas": "Bebidas", "carn": "Carnes", "carnes": "Carnes",
                  "lact": "Lácteos", "lacteos": "Lácteos", "pan": "Panadería", "panaderia": "Panadería",
                  "perf": "Perfumería", "perfumeria": "Perfumería", "snk": "Snacks", "snacks": "Snacks"}
clave = prod.categoria.map(sin_tilde)
sin_mapa = sorted(set(clave) - set(MAPA_CATEGORIA))
cambiadas = (prod.categoria != clave.map(MAPA_CATEGORIA)).sum()
prod["categoria"] = clave.map(MAPA_CATEGORIA)
registrar("P02", "productos", "Categorías unificadas a 8 nombres canónicos (ASEO = Limpieza)",
          "Mismo concepto escrito de varias formas; se valida que cada subcategoría quede en una sola categoría",
          cambiadas, "Cortes por categoría sumarían mal; no afecta la retención", "medido")
pruebas.verificar("P-PROD-01", "Maestro (CSV)", "Toda categoría tiene un nombre canónico", not sin_mapa, len(sin_mapa))
pruebas.verificar("P-PROD-02", "Maestro (CSV)", "Cada subcategoría pertenece a una sola categoría",
                  prod.groupby("subcategoria").categoria.nunique().max() == 1)

prod["nombre"] = prod.nombre.str.replace(r"\s+", " ", regex=True)
prod["precio_lista"] = a_numero(prod.precio_lista)
prod["costo_unitario"] = a_numero(prod.costo_unitario)
registrar("P03", "productos", "Precio y costo convertidos a número; 'N/A' queda como vacío",
          "Formatos 1.223,00 / $1.213 / 1.921 representan el mismo tipo de valor (pesos)",
          prod.costo_unitario.isna().sum() + prod.precio_lista.isna().sum(),
          "El margen de las líneas con costo vacío no se puede calcular (se declara su cobertura)", "medido")

MAPA_ACTIVO = {"si": True, "1": True, "no": False}
prod["activo"] = prod.activo.map(lambda v: MAPA_ACTIVO.get(sin_tilde(v)) if pd.notna(v) else pd.NA).astype("boolean")
n_antes = len(prod)
prod = (prod.assign(_orden=prod.activo.fillna(False).map({True: 0, False: 1}))
            .sort_values(["sku", "_orden"], kind="stable")
            .drop_duplicates("sku", keep="first").drop(columns="_orden"))
registrar("P04", "productos", "SKU repetido: se conserva la versión activa",
          "El maestro debe tener un registro por SKU; la versión inactiva es la desactualizada",
          n_antes - len(prod), "Cambia el precio de lista del SKU-0001 en $33; sin efecto en la P1", "medido")
pruebas.verificar("P-PROD-03", "Maestro (CSV)", "Un registro por SKU", prod.sku.is_unique,
                  prod.sku.duplicated().sum())
pct_costo = prod.costo_unitario.notna().mean()
pruebas.verificar("P-PROD-04", "Maestro (CSV)", "Al menos 95% de los SKU tiene costo unitario (para el margen)",
                  pct_costo >= 0.95, prod.costo_unitario.isna().sum(), severidad="avisa",
                  accion="Seguir; el margen se calcula solo donde hay costo y se declara su cobertura")
salida["productos"] = prod

# COMMAND ----------

# MAGIC %md ## Campañas (C01, C02)

# COMMAND ----------

camp = leer_raw("promo_campanas")


def pct_a_proporcion(v):
    s = str(v).strip().replace("%", "").replace(",", ".")
    x = float(s)
    return x / 100 if x >= 1 else x


camp["descuento_pct"] = camp.descuento_pct.map(pct_a_proporcion)
camp["fecha_ini"] = pd.to_datetime(camp.fecha_ini)
camp["fecha_fin"] = pd.to_datetime(camp.fecha_fin)
camp["canales"] = camp.canales.map(list)
camp["n_skus"] = camp.n_skus.astype(int)
registrar("C01", "campanas", "descuento_pct normalizado a proporción (25% -> 0,25; 50 -> 0,50)",
          "Dos escrituras del mismo campo en el JSON", len(camp), "Ninguno para la P1", "medido")
pruebas.verificar("P-CAMP-01", "Campañas (JSON)", "Descuento entre 0 y 1 y fecha de fin posterior al inicio",
                  camp.descuento_pct.between(0, 1).all() and (camp.fecha_fin >= camp.fecha_ini).all())

csku = leer_raw("promo_campana_sku")
csku["sku"] = csku.sku.str.strip()
csku["sku_en_maestro"] = csku.sku.isin(prod.sku)
registrar("C02", "campana_sku", "SKUs en promoción que no existen en el maestro se conservan marcados",
          "SKU-9001 y SKU-9007 no están en el catálogo; no se inventa su categoría",
          (~csku.sku_en_maestro).sum(), "Ninguno: no aparecen en las ventas de la campaña", "medido")
salida["campanas"], salida["campana_sku"] = camp, csku

# COMMAND ----------

# MAGIC %md ## IPC mensual (I01)

# COMMAND ----------

ipc = leer_raw("ipc_mensual")
ipc["mes"] = (pd.to_datetime(ipc.fecha, utc=True).dt.tz_convert("America/Santiago")
              .dt.tz_localize(None).dt.to_period("M").dt.to_timestamp())
ipc = ipc.sort_values("mes").reset_index(drop=True)
pruebas.verificar("P-IPC-01", "IPC (API)", "12 meses distintos de 2025", ipc.mes.is_unique and len(ipc) == 12)
ipc["var_mensual"] = ipc.valor / 100
ipc["indice_dic2024_100"] = 100 * (1 + ipc.var_mensual).cumprod()
ipc["factor_a_pesos_dic2025"] = ipc.indice_dic2024_100.iloc[-1] / ipc.indice_dic2024_100
ipc = ipc[["mes", "var_mensual", "indice_dic2024_100", "factor_a_pesos_dic2025"]]
registrar("I01", "ipc_mensual", "Índice encadenado desde las variaciones mensuales (base dic-2024 = 100)",
          "La API entrega variación mensual, no nivel; encadenar es la forma estándar de obtener el índice",
          len(ipc), "Los montos en pesos reales cambiarían, no la cantidad de clientes retenidos", "medido")
fuera_rango = (~ipc.var_mensual.between(-0.02, 0.03)).sum()
pruebas.verificar("P-IPC-02", "IPC (API)", "Variación mensual entre -2% y +3%", fuera_rango == 0, fuera_rango,
                  severidad="avisa", accion="Seguir; revisar si la API cambió de unidad")
salida["ipc_mensual"] = ipc

# COMMAND ----------

# MAGIC %md ## Tiendas, comunas, regiones y medios de pago (T00, T01)

# COMMAND ----------

MACROZONA = {"Metropolitana": "Metropolitana", "Valparaíso": "Centro sur", "Biobío": "Centro sur",
             "Araucanía": "Sur", "Los Lagos": "Sur"}
regiones = leer_raw("pos_regiones")
comunas = leer_raw("pos_comunas")
huerfanas = comunas[~comunas.region_id.isin(regiones.region_id)]
CORRECCION_REGION = {"La Florida": "1"}
for comuna, region_ok in CORRECCION_REGION.items():
    comunas.loc[comunas.comuna == comuna, "region_id"] = region_ok
registrar("T00", "comunas", "La Florida apuntaba a una región inexistente (region_id 9); se asigna a Metropolitana",
          "La Florida es comuna de Santiago y el 92% de las boletas de su tienda dicen Metropolitana",
          len(huerfanas), "Sin corrección, la tienda Express La Florida y sus 111 clientes quedarían sin región",
          "medido")
pruebas.verificar("P-GEO-01", "POS (SQLite)", "Ninguna comuna apunta a una región inexistente",
                  comunas.region_id.isin(regiones.region_id).all(),
                  (~comunas.region_id.isin(regiones.region_id)).sum())

tiendas = (leer_raw("pos_tiendas").merge(comunas, on="comuna_id", how="left").merge(regiones, on="region_id", how="left"))
tiendas["macrozona"] = tiendas.region.map(MACROZONA)
for c in ["tienda_id", "comuna_id", "region_id"]:
    tiendas[c] = tiendas[c].astype(int)
registrar("T01", "tiendas", "Macrozona asignada según el README del caso",
          "Es la agrupación que usa la empresa para describir sus locales", len(tiendas),
          "Solo cambia la agrupación regional de los cortes", "verbalizado (README del caso)")
pruebas.verificar("P-GEO-02", "POS (SQLite)", "Toda tienda tiene macrozona", tiendas.macrozona.notna().all(),
                  tiendas.macrozona.isna().sum())

medios = leer_raw("pos_medios_pago")
medios["medio_id"] = medios.medio_id.astype(int)
comunas["comuna_id"] = comunas.comuna_id.astype(int)
comunas["region_id"] = comunas.region_id.astype(int)
regiones["region_id"] = regiones.region_id.astype(int)
regiones["macrozona"] = regiones.region.map(MACROZONA)
salida.update(tiendas=tiendas, comunas=comunas, regiones=regiones, medios_pago=medios)

# COMMAND ----------

# MAGIC %md ## Clientes (CL01, CL02)

# COMMAND ----------

cli = leer_raw("pos_clientes")
cli["cliente_id"] = cli.cliente_id.astype(int)
cli["comuna_id"] = cli.comuna_id.astype(int)
cli["nombre"] = cli.nombre.str.strip()
cli["fecha_alta"] = cli.fecha_alta.map(parsear_fecha)
pruebas.verificar("P-CLI-01", "POS (SQLite)", "Toda fecha de alta se pudo interpretar", cli.fecha_alta.notna().all(),
                  cli.fecha_alta.isna().sum())


def normalizar_rut(txt):
    s = re.sub(r"[^0-9kK]", "", str(txt)).upper()
    if len(s) < 2:
        return pd.NA, False
    cuerpo, dv = s[:-1], s[-1]
    suma, mult = 0, 2
    for d in reversed(cuerpo):
        suma += int(d) * mult
        mult = 2 if mult == 7 else mult + 1
    resto = 11 - suma % 11
    esperado = "0" if resto == 11 else "K" if resto == 10 else str(resto)
    return f"{cuerpo}-{dv}", dv == esperado


ruts = cli.rut.map(normalizar_rut)
cli["rut"] = ruts.map(lambda t: t[0])
cli["rut_valido"] = ruts.map(lambda t: t[1])
registrar("CL01", "clientes", "RUT normalizado a 12345678-K y dígito verificador validado",
          "Más de 30 formatos para el mismo dato", (~cli.rut_valido).sum(),
          "Ninguno para la P1: el cliente se identifica por cliente_id, no por RUT", "medido")
pruebas.verificar("P-CLI-02", "POS (SQLite)", "Un cliente por RUT", cli.rut.is_unique, cli.rut.duplicated().sum())
registrar("CL02", "clientes", "Segmento se usa como atributo nominal con el estado actual (sin historial)",
          "Correo de Marketing: no hay regla ni beneficios por segmento, se sobrescribe y los tres compran parecido",
          len(cli), "Si el segmento ordenara el valor del cliente, el corte por segmento ocultaría esa diferencia",
          "verbalizado (correo de Marketing) y medido")
salida["clientes"] = cli

# COMMAND ----------

# MAGIC %md ## Boletas (B01 a B06)

# COMMAND ----------

bol_raw = leer_raw("pos_boletas")
n_origen_boletas = len(bol_raw)
bol = bol_raw.drop_duplicates().copy()
registrar("B01", "boletas", "Se elimina la copia de las boletas duplicadas exactas",
          "Misma boleta_id y mismos valores en todas las columnas: es una carga doble",
          n_origen_boletas - len(bol), "Se sobrecontarían 62 visitas", "medido")
pruebas.verificar("P-BOL-01", "POS (SQLite)", "boleta_id único después de quitar duplicados exactos",
                  bol.boleta_id.is_unique, bol.boleta_id.duplicated().sum())

bol["fecha_origen"] = bol.fecha
dmy = bol.fecha.str.strip().str.extract(r"^(\d{1,2})/(\d{1,2})/\d{4}$").dropna().astype(int)
pruebas.verificar("P-BOL-02", "POS (SQLite)", "Ninguna fecha dd/mm/aaaa contradice la lectura día/mes",
                  (dmy[1] > 12).sum() == 0, (dmy[1] > 12).sum())
bol["fecha"] = bol.fecha.map(parsear_fecha)
pruebas.verificar("P-BOL-03", "POS (SQLite)", "Toda fecha de boleta se pudo interpretar", bol.fecha.notna().all(),
                  bol.fecha.isna().sum())
registrar("B02", "boletas", "Fechas unificadas desde 5 formatos; dd/mm/aaaa leído como día/mes",
          "Formato chileno; ningún valor tiene un mes mayor que 12 en la segunda posición", len(bol),
          "Si fuera mes/día, se moverían de mes boletas con día <= 12", "medido")

fuera = bol.fecha.dt.year != 2025
registrar("B03", "boletas", "Se excluyen boletas con fecha fuera de 2025 (año 2026)",
          "Los datos del caso son de 2025; la columna mes tampoco calza con esas fechas",
          fuera.sum(), "Si fueran ventas de 2025 mal digitadas, faltaría un 0,6% de las visitas", "medido")
bol = bol[~fuera].copy()
mes_distinto = bol.mes.astype(str).str.strip() != bol.fecha.dt.month.astype(str)
registrar("B04", "boletas", "Se descarta la columna mes; manda la fecha",
          "Contradice a la fecha en cientos de casos; la fecha es el dato primario", mes_distinto.sum(),
          "Si mes fuera la correcta, cambiaría el mes asignado a esas boletas", "medido")

bol["tienda_id"] = bol.tienda_id.astype(int)
bol["medio_id"] = bol.medio_id.astype(int)
reg_tienda = bol.tienda_id.map(tiendas.set_index("tienda_id").region)
registrar("B05", "boletas", "Se descarta la columna region de la boleta; la región sale de la tienda",
          "No coincide con la región del local en compras presenciales, donde la región es la del local",
          (bol.region != reg_tienda).sum(), "Cambiaría la región de esas boletas en los cortes regionales", "medido")
bol["cliente_id"] = pd.to_numeric(bol.cliente_id).astype("Int64")
bol["es_anonima"] = bol.cliente_id.isna()
registrar("B06", "boletas", "Boletas anónimas: se conservan en totales y fuera de la retención",
          "Sin cliente_id no se puede seguir a nadie en el tiempo; correo de Marketing: son compras fuera del programa",
          bol.es_anonima.sum(), "Si fueran clientes frecuentes sin tarjeta, la retención real sería mayor que la medida",
          "verbalizado (acta y correo) y medido")
bol["boleta_id"] = bol.boleta_id.astype(int)
bol = bol[["boleta_id", "fecha", "fecha_origen", "tienda_id", "cliente_id", "es_anonima", "canal", "medio_id"]]
huerf_cli = (~bol.cliente_id.dropna().isin(cli.cliente_id)).sum()
pruebas.verificar("P-BOL-04", "POS (SQLite)", "Todo cliente_id de las boletas existe en clientes", huerf_cli == 0,
                  huerf_cli)
huerf_tie = (~bol.tienda_id.isin(tiendas.tienda_id)).sum()
pruebas.verificar("P-BOL-05", "POS (SQLite)", "Toda tienda de las boletas existe en tiendas", huerf_tie == 0,
                  huerf_tie)

# COMMAND ----------

# MAGIC %md ## Detalle de boletas (D01 a D07) y visita (B07)

# COMMAND ----------

det_raw = leer_raw("pos_boleta_detalle")
det = det_raw.copy()
det["boleta_id"] = det.boleta_id.astype(int)
det = det[det.boleta_id.isin(bol.boleta_id)].copy()
registrar("D01", "detalle", "Se excluyen las líneas de boletas excluidas (fuera de 2025)",
          "Siguen a su boleta", len(det_raw) - len(det), "Igual que B03", "medido")

det["linea_origen"] = det.linea.astype(int)
repetidas = det.duplicated(["boleta_id", "linea_origen"], keep=False)
iguales = det.duplicated(keep=False)
det = det.sort_values(["boleta_id", "linea_origen"], kind="stable")
det["linea_id"] = det.groupby("boleta_id").cumcount() + 1
registrar("D02", "detalle", "Número de línea repetido: se conservan las líneas y se crea linea_id",
          "Las líneas con número repetido tienen otro SKU y otro precio: son productos distintos, no copias",
          det.duplicated(["boleta_id", "linea_origen"]).sum(),
          "Si fueran copias, se sobrecontaría ingreso (no visitas ni clientes)", "medido")
pruebas.verificar("P-DET-01", "POS (SQLite)", "Ninguna línea con número repetido es copia exacta",
                  not (repetidas & iguales).any(), (repetidas & iguales).sum())

det["sku"] = det.sku.str.strip()
det["cantidad"] = det.cantidad.astype(int)
det["precio_unitario"] = a_numero(det.precio_unitario)
det["monto_linea"] = a_numero(det.monto_linea)
sin_monto = det[["precio_unitario", "monto_linea"]].isna().any(axis=1).sum()
pruebas.verificar("P-DET-02", "POS (SQLite)", "Todo precio y monto de línea es numérico", sin_monto == 0, sin_monto)


def normalizar_descuento(txt):
    s = str(txt).strip().replace(",", ".")
    v = float(s[:-1]) / 100 if s.endswith("%") else float(s)
    return round(v / 100 if v >= 1 else v, 4)


det["descuento"] = det.descuento.map(normalizar_descuento)
otros = (~det.descuento.isin([0.0, 0.10, 0.25])).sum()
pruebas.verificar("P-DET-03", "POS (SQLite)", "Descuento de línea en {0; 0,10; 0,25}", otros == 0, otros)
registrar("D03", "detalle", "Descuento normalizado a 0 / 0,10 / 0,25",
          "Correo de Marketing: 10%, 10 y 0,10 son el mismo 10%", (det.descuento > 0).sum(),
          "Solo afecta montos esperados y el cuadre", "verbalizado (correo de Marketing)")
det["es_devolucion"] = det.cantidad < 0
registrar("D04", "detalle", "Cantidad negativa = devolución; resta del monto de la boleta",
          "Todas tienen monto negativo y cuadran con precio x cantidad", det.es_devolucion.sum(),
          "Si no fueran devoluciones, el ingreso neto estaría subestimado", "inferido")
det["monto_esperado"] = (det.precio_unitario * det.cantidad * (1 - det.descuento)).round(0)
det["linea_cuadra"] = (det.monto_linea - det.monto_esperado).abs() <= 1
registrar("D05", "detalle", "Líneas que no cuadran: no se corrigen, se marcan y se reportan",
          "Correo de Marketing: no forzar; el ingreso usa monto_linea (lo cobrado)", (~det.linea_cuadra).sum(),
          "Si el monto correcto fuera el esperado, el ingreso bajaría; la retención (conteo de clientes) no cambia",
          "verbalizado (correo de Marketing) y medido")

x = (det[["boleta_id", "linea_id", "sku"]]
     .merge(bol[["boleta_id", "fecha", "canal"]], on="boleta_id")
     .merge(csku[["campana_id", "sku"]], on="sku")
     .merge(camp[["campana_id", "fecha_ini", "fecha_fin", "canales"]], on="campana_id"))
x = x[(x.fecha >= x.fecha_ini.dt.normalize()) & (x.fecha <= x.fecha_fin)
      & x.apply(lambda r: r.canal in r.canales, axis=1)]
pruebas.verificar("P-DET-04", "Campañas (JSON)", "Ninguna línea cae en dos campañas",
                  not x.duplicated(["boleta_id", "linea_id"]).any(), x.duplicated(["boleta_id", "linea_id"]).sum())
det = det.merge(x[["boleta_id", "linea_id", "campana_id"]], on=["boleta_id", "linea_id"], how="left")
registrar("D06", "detalle", "Campaña asignada por SKU + fecha + canal, no por el descuento de la línea",
          "Correo de Marketing: el 2x1 no queda guardado como 50% en la línea", det.campana_id.notna().sum(),
          "Solo afecta el análisis de promociones (P3, fuera de alcance)", "verbalizado (correo de Marketing)")
det["sku_en_maestro"] = det.sku.isin(prod.sku)
registrar("D07", "detalle", "Líneas con SKU que no está en el maestro: se conservan, sin atributos de producto",
          "Son ventas reales; el catálogo no está homologado (acta)", (~det.sku_en_maestro).sum(),
          "Ninguno para la P1; los cortes por categoría quedan con un grupo 'Sin maestro'",
          "verbalizado (acta) y medido")
det = det[["boleta_id", "linea_id", "linea_origen", "sku", "cantidad", "precio_unitario", "descuento",
           "monto_linea", "monto_esperado", "linea_cuadra", "es_devolucion", "campana_id", "sku_en_maestro"]]

resumen = det.groupby("boleta_id").agg(monto_boleta=("monto_linea", "sum"), n_lineas=("linea_id", "size"),
                                       tiene_devolucion=("es_devolucion", "any"))
bol = bol.merge(resumen, on="boleta_id", how="left")
pruebas.verificar("P-BOL-06", "POS (SQLite)", "Toda boleta válida tiene al menos una línea",
                  bol.monto_boleta.notna().all(), bol.monto_boleta.isna().sum())
bol["es_visita"] = bol.monto_boleta > 0
registrar("B07", "boletas", "Una boleta es visita solo si su monto neto es positivo",
          "Una devolución sin compra no es una visita de compra", (~bol.es_visita).sum(),
          "Si contaran, algunos clientes parecerían activos sin haber comprado", "inferido")
pruebas.verificar("P-BOL-07", "POS (SQLite)", "Todas las boletas válidas son de 2025",
                  (bol.fecha.dt.year == 2025).all(), (bol.fecha.dt.year != 2025).sum())
salida.update(boletas=bol, detalle=det)

# COMMAND ----------

# MAGIC %md ## Cobertura de datos

# COMMAND ----------

n_dup = n_origen_boletas - len(bol_raw.drop_duplicates())
n_fuera, n_validas = int(fuera.sum()), len(bol)
n_anon = int(bol.es_anonima.sum())
n_no_visita_ident = int((~bol.es_anonima & ~bol.es_visita).sum())
n_usadas = n_validas - n_anon - n_no_visita_ident
pasos = [("Boletas en origen", n_origen_boletas, ""), ("(-) Duplicadas exactas", -n_dup, "B01"),
         ("(-) Fuera de 2025", -n_fuera, "B03"), ("= Boletas válidas 2025", n_validas, ""),
         ("(-) Anónimas (sin cliente_id)", -n_anon, "B06"),
         ("(-) Identificadas sin monto positivo", -n_no_visita_ident, "B07"),
         ("= Visitas identificadas usadas en la retención", n_usadas, "")]
cobertura = pd.DataFrame(pasos, columns=["paso", "boletas", "regla"])
cobertura["pct_del_origen"] = (cobertura.boletas.abs() / n_origen_boletas * 100).round(1)
cobertura["orden"] = range(1, len(cobertura) + 1)
monto_total = bol.monto_boleta.sum()
monto_ident = bol.loc[~bol.es_anonima, "monto_boleta"].sum()
print(cobertura)
print(f"Ingreso cubierto por clientes identificados: {monto_ident / monto_total:.1%}")
pruebas.verificar("P-COB-01", "Cobertura", "Al menos 80% de las boletas de origen entra a la retención",
                  n_usadas / n_origen_boletas >= 0.80, n_origen_boletas - n_usadas, severidad="avisa",
                  accion="Seguir; revisar qué regla excluye más registros que en la corrida anterior")

# COMMAND ----------

# MAGIC %md ## Publicación

# COMMAND ----------

pruebas.cerrar()
for tabla, df in salida.items():
    escribir_pandas(df, "plata", tabla)
escribir_pandas(cobertura, "control", "cobertura")
bit.guardar()
