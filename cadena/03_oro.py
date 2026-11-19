# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa oro: modelo dimensional
# MAGIC Esquema en estrella con `fact_venta_linea` y cinco dimensiones.
# MAGIC
# MAGIC **Grano: una fila es una línea de boleta: un producto (SKU) vendido o devuelto en una boleta válida de 2025.**
# MAGIC
# MAGIC El modelo se publica solo si pasan las pruebas que protegen la métrica.

# COMMAND ----------

import numpy as np
import pandas as pd

from comun import Bitacora, Pruebas, a_pandas, escribir_pandas

bit = Bitacora("oro")
registrar = bit.registrar
pruebas = Pruebas("oro")

bol_p, det_p, cli_p = a_pandas("plata", "boletas"), a_pandas("plata", "detalle"), a_pandas("plata", "clientes")
tie_p, med_p = a_pandas("plata", "tiendas"), a_pandas("plata", "medios_pago")
prod_p, camp_p, ipc_p = a_pandas("plata", "productos"), a_pandas("plata", "campanas"), a_pandas("plata", "ipc_mensual")
cobertura = a_pandas("control", "cobertura")
n_usadas = int(cobertura.loc[cobertura.paso.str.startswith("= Visitas"), "boletas"].iloc[0])

# COMMAND ----------

# MAGIC %md ## Dimensiones

# COMMAND ----------

dias = pd.DataFrame({"fecha": pd.date_range("2025-01-01", "2025-12-31", freq="D")})
dias["mes_inicio"] = dias.fecha.dt.to_period("M").dt.to_timestamp()
ipc_p["mes_inicio"] = pd.to_datetime(ipc_p.mes)
dim_fecha = dias.merge(ipc_p, on="mes_inicio", how="left")
pruebas.verificar("O-FEC-01", "IPC (API)", "Todo día de 2025 tiene IPC", dim_fecha.factor_a_pesos_dic2025.notna().all(),
                  dim_fecha.factor_a_pesos_dic2025.isna().sum())
dim_fecha = pd.DataFrame({
    "fecha_key": dim_fecha.fecha.dt.strftime("%Y%m%d").astype(int),
    "fecha": dim_fecha.fecha,
    "mes": dim_fecha.fecha.dt.month,
    "trimestre": "T" + dim_fecha.fecha.dt.quarter.astype(str),
    "ipc_var_mensual": dim_fecha.var_mensual,
    "factor_a_pesos_dic2025": dim_fecha.factor_a_pesos_dic2025,
})
registrar("M07", "dim_fecha", "dim_fecha con fecha, mes, trimestre e IPC: sin semestre, año, semana, día de la semana, "
          "feriado ni nombre del mes",
          "La métrica corta por fecha con ventanas móviles; el trimestre lo usa la prueba K04. Ningún cálculo usa el "
          "resto; el año es constante y el feriado exigiría una fuente fuera del caso", len(dim_fecha),
          "Ninguno para la P1: se derivan de la fecha y se agregan sin tocar el hecho si una pregunta los usa", "inferido")

canal_por_tienda = bol_p.groupby("tienda_id").canal.agg(["nunique", "first"])
dim_tienda = tie_p.rename(columns={"nombre": "tienda", "tienda_id": "tienda_key"})[
    ["tienda_key", "tienda", "formato", "comuna", "region", "macrozona"]].copy()
dim_tienda["canal"] = dim_tienda.tienda_key.map(canal_por_tienda["first"])
pruebas.verificar("O-TIE-01", "POS (SQLite)", "Cada tienda vende por un solo canal y solo el formato Online es Web",
                  (canal_por_tienda["nunique"] == 1).all() and dim_tienda.canal.eq("Web").eq(
                      dim_tienda.formato.eq("Online")).all())
registrar("M04", "dim_tienda", "El canal se modela como atributo de la tienda y no como dimensión propia",
          "Toda boleta Web es de la Tienda Online y viceversa. Formato = tipo de local (Híper, Súper, Express u "
          "Online); canal = cómo se compra (Presencial o Web)", len(dim_tienda),
          "Si una tienda física vendiera por web, habría que volver a una dimensión de canal", "medido")

mp = bol_p.merge(med_p, on="medio_id")
multi_medio = (mp[mp.es_visita & ~mp.es_anonima].groupby("cliente_id").medio.nunique() > 1).mean()
registrar("M05", "modelo", "El medio de pago queda fuera del modelo dimensional",
          f"No distingue comportamiento (cada medio ~25%, misma proporción de anónimas y ticket similar; "
          f"{multi_medio:.0%} de los clientes usa más de uno); no aporta a la P1",
          len(med_p), "Si el medio de pago explicara la retención, el modelo no permitiría verlo; se puede "
          "reincorporar desde plata", "medido")

visitas_id = bol_p[bol_p.es_visita & ~bol_p.es_anonima].copy()
visitas_id["cliente_id"] = visitas_id.cliente_id.astype(int)
resumen_cli = visitas_id.groupby("cliente_id").agg(primera_visita_2025=("fecha", "min"))
dim_cliente = cli_p.merge(resumen_cli, left_on="cliente_id", right_index=True, how="left")
dim_cliente["tipo_cliente"] = np.where(dim_cliente.fecha_alta < pd.Timestamp("2025-01-01"), "Base heredada", "Nuevo 2025")
alta_posterior = int((dim_cliente.fecha_alta > dim_cliente.primera_visita_2025).sum())
dim_cliente = dim_cliente.rename(columns={"cliente_id": "cliente_key"})[
    ["cliente_key", "tipo_cliente", "segmento", "fecha_alta", "primera_visita_2025"]]
anonimo = pd.DataFrame([{"cliente_key": 0, "tipo_cliente": "Anónimo", "segmento": "Sin programa (anónimo)",
                         "fecha_alta": pd.NaT, "primera_visita_2025": pd.NaT}])
dim_cliente = pd.concat([anonimo, dim_cliente], ignore_index=True)
dim_cliente["cliente_key"] = dim_cliente.cliente_key.astype(int)
reales = dim_cliente.cliente_key > 0
registrar("M01", "dim_cliente", "Tipo de cliente según fecha de alta: antes de 2025 = Base heredada; en 2025 = Nuevo 2025",
          "Los datos solo cubren 2025: la primera boleta del año no distingue a un cliente nuevo de uno antiguo que "
          "vuelve. Sensibilidad (K05): sin los clientes con alta posterior a su primera compra, la brecha entre "
          "tipos se reduce", (dim_cliente[reales].tipo_cliente == "Base heredada").sum(),
          "Si la fecha de alta no reflejara la antigüedad, se mezclarían retenidos con adquiridos", "medido e inferido")
registrar("M02", "dim_cliente", "Clientes con fecha de alta posterior a su primera visita: se conservan sin corregir el "
          "alta y se marcan para la sensibilidad K05",
          "Contraparte: el alta 'es la primera compra', con advertencia de revisarlo; los datos lo contradicen",
          alta_posterior, "Todos tienen el alta en 2025: la base heredada no cambia, pero la tasa de los nuevos sí "
          "(ver K05)", "verbalizado (acta) y medido")
registrar("M06", "dim_cliente", "dim_cliente sin RUT ni nombre y sin conteos de visitas",
          "Minimización (Ley 21.719, clase 3): la P1 se responde con cliente_id. Los conteos son medidas y van en el "
          "hecho (clase 10)", int(reales.sum()),
          "Ninguno: RUT y nombre siguen en plata.clientes si una pregunta futura los necesita", "inferido")

dim_producto = prod_p[["sku", "nombre", "categoria", "subcategoria"]].copy()
dim_producto["en_maestro"] = True
skus_sin_maestro = sorted(set(det_p.sku) - set(dim_producto.sku))
faltantes_prod = pd.DataFrame({"sku": skus_sin_maestro, "nombre": "SKU sin maestro", "categoria": "Sin maestro",
                               "subcategoria": "Sin maestro", "en_maestro": False})
dim_producto = pd.concat([dim_producto, faltantes_prod], ignore_index=True)
dim_producto.insert(0, "producto_key", range(1, len(dim_producto) + 1))
registrar("M03", "dim_producto", "Cada SKU vendido que no está en el maestro obtiene su fila con categoría 'Sin maestro'",
          "Conserva el código del SKU y evita perder ventas en el cruce con productos", len(skus_sin_maestro),
          "Ninguno para la P1; esas líneas no tienen costo y quedan fuera del margen (se declara la cobertura)",
          "medido")

dim_campana = camp_p[["campana_id", "nombre", "tipo", "fecha_ini", "fecha_fin", "descuento_pct"]].copy()
dim_campana["fecha_ini"] = pd.to_datetime(dim_campana.fecha_ini).dt.normalize()
dim_campana["fecha_fin"] = pd.to_datetime(dim_campana.fecha_fin).dt.normalize()
dim_campana.insert(0, "campana_key", range(1, len(dim_campana) + 1))
sin_camp = pd.DataFrame([{"campana_key": 0, "campana_id": "SIN", "nombre": "Sin campaña", "tipo": "Ninguna",
                          "fecha_ini": pd.NaT, "fecha_fin": pd.NaT, "descuento_pct": 0.0}])
dim_campana = pd.concat([sin_camp, dim_campana], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Tabla de hechos `fact_venta_linea`

# COMMAND ----------

f = det_p.merge(bol_p[["boleta_id", "fecha", "tienda_id", "cliente_id"]], on="boleta_id", how="left")
pruebas.verificar("O-HEC-01", "Modelo", "Toda línea tiene su boleta", f.fecha.notna().all(), f.fecha.isna().sum())
f["fecha_key"] = f.fecha.dt.strftime("%Y%m%d").astype(int)
f["cliente_key"] = f.cliente_id.fillna(0).astype(int)
f["tienda_key"] = f.tienda_id.astype(int)
f["producto_key"] = f.sku.map(dim_producto.set_index("sku").producto_key)
f["campana_key"] = f.campana_id.map(dim_campana.set_index("campana_id").campana_key).fillna(0).astype(int)
factor = f.fecha_key.map(dim_fecha.set_index("fecha_key").factor_a_pesos_dic2025)
f["monto_linea_real_dic2025"] = (f.monto_linea * factor).round(0)
f["costo_linea"] = (f.cantidad * f.sku.map(prod_p.set_index("sku").costo_unitario)).round(0)
f["margen_linea"] = f.monto_linea - f.costo_linea
f["margen_linea_real_dic2025"] = (f.margen_linea * factor).round(0)
f["tiene_costo"] = f.costo_linea.notna()

fact_venta_linea = f[["boleta_id", "linea_id", "fecha_key", "cliente_key", "tienda_key", "producto_key",
                      "campana_key", "cantidad", "monto_linea", "monto_linea_real_dic2025", "costo_linea",
                      "margen_linea", "margen_linea_real_dic2025", "tiene_costo", "linea_cuadra"]].copy()
pct_costo = fact_venta_linea.loc[fact_venta_linea.tiene_costo, "monto_linea"].sum() / fact_venta_linea.monto_linea.sum()
registrar("M08", "fact_venta_linea", "El margen de la línea = monto cobrado - cantidad x costo unitario del maestro; "
          "sin costo, la línea queda fuera del margen",
          "El caso de negocio se expresa en margen y no en venta. El maestro tiene costo para el "
          + f"{pct_costo * 100:.1f}".replace(".", ",") + "% de la venta", int((~fact_venta_linea.tiene_costo).sum()),
          "Si el costo del maestro no fuera el costo real, el margen cambiaría; la retención no", "medido")

# COMMAND ----------

# MAGIC %md ## Pruebas que protegen la métrica

# COMMAND ----------

dup = fact_venta_linea.duplicated(["boleta_id", "linea_id"]).sum()
pruebas.verificar("O-HEC-02", "Métrica", "Clave del hecho (boleta_id, linea_id) única", dup == 0, dup)
pruebas.verificar("O-HEC-03", "Métrica", "Mismas filas en el hecho que en plata.detalle",
                  len(fact_venta_linea) == len(det_p), abs(len(fact_venta_linea) - len(det_p)))
for dim, llave, tabla in [(dim_fecha, "fecha_key", "dim_fecha"), (dim_cliente, "cliente_key", "dim_cliente"),
                          (dim_tienda, "tienda_key", "dim_tienda"), (dim_producto, "producto_key", "dim_producto"),
                          (dim_campana, "campana_key", "dim_campana")]:
    pruebas.verificar(f"O-DIM-{tabla}", "Métrica", f"Llave única en {tabla}", dim[llave].is_unique,
                      dim[llave].duplicated().sum())
    huerf = int((~fact_venta_linea[llave].isin(dim[llave])).sum())
    pruebas.verificar(f"O-HUE-{tabla}", "Métrica", f"Sin claves huérfanas hacia {tabla}", huerf == 0, huerf)
dif = abs(fact_venta_linea.monto_linea.sum() - bol_p.monto_boleta.sum())
pruebas.verificar("O-HEC-04", "Métrica", "El monto del hecho cuadra al peso con plata.boletas", dif < 1, 0)
fuera = int((~fact_venta_linea.fecha_key.between(20250101, 20251231)).sum())
pruebas.verificar("O-HEC-05", "Métrica", "Todas las fechas del hecho están dentro de 2025", fuera == 0, fuera)
cero = int((fact_venta_linea.cantidad == 0).sum())
pruebas.verificar("O-HEC-06", "Métrica", "Ninguna línea con cantidad 0", cero == 0, cero)
b = fact_venta_linea.groupby(["boleta_id", "fecha_key", "cliente_key", "tienda_key"], as_index=False).monto_linea.sum()
pruebas.verificar("O-HEC-07", "Métrica", "Cada boleta tiene una sola fecha, tienda y cliente",
                  b.boleta_id.is_unique, b.boleta_id.duplicated().sum())
visitas = int(((b.monto_linea > 0) & (b.cliente_key != 0)).sum())
pruebas.verificar("O-HEC-08", "Métrica", "Visitas identificadas del modelo = cobertura de plata", visitas == n_usadas,
                  abs(visitas - n_usadas))
pruebas.verificar("O-HEC-09", "Margen", "El costo cubre al menos 95% de la venta", pct_costo >= 0.95,
                  int((~fact_venta_linea.tiene_costo).sum()), severidad="avisa",
                  accion="Seguir; el margen se informa con su cobertura")

# COMMAND ----------

# MAGIC %md ## Diccionario de datos (campo, significado, origen, regla y unidad)

# COMMAND ----------

POS, CSV, JSN, API, CAL = "POS (SQLite)", "Maestro de productos (CSV)", "Campañas (JSON)", "API mindicador.cl", "Calendario"
D = [
 ("fact_venta_linea", "boleta_id", "Número de boleta (dimensión degenerada)", POS + " boletas.boleta_id", "Sin duplicados exactos (B01); solo 2025 (B03)", "código"),
 ("fact_venta_linea", "linea_id", "Número de línea dentro de la boleta", "Creado en la limpieza", "Correlativo por boleta; reemplaza al número de línea repetido (D02)", "código"),
 ("fact_venta_linea", "fecha_key", "Día de la compra", POS + " boletas.fecha", "5 formatos unificados (B02) a aaaammdd", "aaaammdd"),
 ("fact_venta_linea", "cliente_key", "Cliente del programa (0 = anónimo)", POS + " boletas.cliente_id", "Nulo pasa a 0 (B06)", "código"),
 ("fact_venta_linea", "tienda_key", "Local de la compra", POS + " boletas.tienda_id", "A entero", "código"),
 ("fact_venta_linea", "producto_key", "Producto vendido", POS + " detalle.sku + " + CSV, "SKU sin maestro con miembro propio (M03)", "código"),
 ("fact_venta_linea", "campana_key", "Campaña que aplica (0 = sin campaña)", JSN, "Por SKU + fecha + canal (D06)", "código"),
 ("fact_venta_linea", "cantidad", "Unidades; negativa = devolución", POS + " detalle.cantidad", "A entero (D04)", "unidades"),
 ("fact_venta_linea", "monto_linea", "Monto cobrado", POS + " detalle.monto_linea", "Texto con punto de miles a número; no se corrige si no cuadra (D05)", "$ nominales"),
 ("fact_venta_linea", "monto_linea_real_dic2025", "Monto cobrado en pesos de diciembre de 2025", "monto_linea + " + API, "monto_linea x factor del mes (I01)", "$ dic-2025"),
 ("fact_venta_linea", "costo_linea", "Costo de la línea", "cantidad + " + CSV + " costo_unitario", "cantidad x costo unitario; vacío si el SKU no tiene costo (M08)", "$ nominales"),
 ("fact_venta_linea", "margen_linea", "Margen bruto de la línea", "monto_linea y costo_linea", "monto_linea - costo_linea (M08)", "$ nominales"),
 ("fact_venta_linea", "margen_linea_real_dic2025", "Margen bruto en pesos de diciembre de 2025", "margen_linea + " + API, "margen_linea x factor del mes", "$ dic-2025"),
 ("fact_venta_linea", "tiene_costo", "La línea tiene costo en el maestro", CSV, "costo_linea no vacío", "sí/no"),
 ("fact_venta_linea", "linea_cuadra", "El monto calza con precio x cantidad x (1 - descuento)", POS + " detalle", "Diferencia de hasta $1 (D05)", "sí/no"),
 ("dim_fecha", "fecha_key", "Llave del día", CAL, "aaaammdd", "aaaammdd"),
 ("dim_fecha", "fecha", "Fecha calendario de 2025", CAL, "1-ene a 31-dic de 2025", "fecha"),
 ("dim_fecha", "mes", "Mes del año", CAL, "Mes de la fecha", "1 a 12"),
 ("dim_fecha", "trimestre", "Trimestre calendario", CAL, "T1 a T4 (lo usa K04)", "texto"),
 ("dim_fecha", "ipc_var_mensual", "Variación mensual del IPC", API, "Porcentaje / 100; fecha UTC a hora de Chile", "proporción"),
 ("dim_fecha", "factor_a_pesos_dic2025", "Factor para expresar un monto en pesos de dic-2025", API, "Índice encadenado base dic-2024 = 100 (I01)", "factor"),
 ("dim_cliente", "cliente_key", "Cliente del programa (0 = anónimo)", POS + " clientes.cliente_id", "Miembro especial 0 = Anónimo", "código"),
 ("dim_cliente", "tipo_cliente", "Base heredada o Nuevo 2025", POS + " clientes.fecha_alta", "Alta antes de 2025 = Base heredada (M01)", "texto"),
 ("dim_cliente", "segmento", "Clásico, Plus o Black (estado actual)", POS + " clientes.segmento", "Tipo 1, sin historial (CL02)", "texto"),
 ("dim_cliente", "fecha_alta", "Inscripción en la tarjeta", POS + " clientes.fecha_alta", "Dos formatos a fecha; no se corrige (M02)", "fecha"),
 ("dim_cliente", "primera_visita_2025", "Primera visita del año (adquisición de los nuevos)", POS + " boletas", "Mínima fecha con monto neto > 0", "fecha"),
 ("dim_tienda", "tienda_key", "Local", POS + " tiendas.tienda_id", "A entero", "código"),
 ("dim_tienda", "tienda", "Nombre del local", POS + " tiendas.nombre", "Sin cambios", "texto"),
 ("dim_tienda", "formato", "Tipo de local: Híper, Súper, Express u Online", POS + " tiendas.formato", "Sin cambios", "texto"),
 ("dim_tienda", "canal", "Cómo se compra: Presencial o Web", POS + " boletas.canal", "Único por tienda (M04)", "texto"),
 ("dim_tienda", "comuna", "Comuna del local", POS + " comunas", "Sin cambios", "texto"),
 ("dim_tienda", "region", "Región del local", POS + " regiones", "La Florida corregida a Metropolitana (T00)", "texto"),
 ("dim_tienda", "macrozona", "Metropolitana, Centro sur o Sur", "README del caso", "Mapa región -> macrozona (T01)", "texto"),
 ("dim_producto", "producto_key", "Producto", "Creado en el modelo", "Correlativo", "código"),
 ("dim_producto", "sku", "Código del producto", CSV + " y " + POS, "Un registro por SKU (P04)", "código"),
 ("dim_producto", "nombre", "Nombre del producto", CSV, "Espacios normalizados", "texto"),
 ("dim_producto", "categoria", "Categoría (8)", CSV, "Nombres canónicos (P02)", "texto"),
 ("dim_producto", "subcategoria", "Subcategoría", CSV, "Sin cambios; valida la categoría", "texto"),
 ("dim_producto", "en_maestro", "El SKU existe en el catálogo", CSV, "Falso = 'Sin maestro' (M03)", "sí/no"),
 ("dim_campana", "campana_key", "Campaña (0 = sin campaña)", "Creado en el modelo", "Miembro especial 0", "código"),
 ("dim_campana", "campana_id", "Código de la campaña", JSN, "Sin cambios", "código"),
 ("dim_campana", "nombre", "Nombre de la campaña", JSN, "Sin cambios", "texto"),
 ("dim_campana", "tipo", "Descuento directo o 2x1", JSN, "Sin cambios", "texto"),
 ("dim_campana", "fecha_ini", "Inicio de la campaña", JSN, "A fecha", "fecha"),
 ("dim_campana", "fecha_fin", "Último día de la campaña (inclusive)", JSN, "A fecha", "fecha"),
 ("dim_campana", "descuento_pct", "Descuento declarado", JSN, "25% y 50 a proporción (C01)", "proporción"),
]
diccionario = pd.DataFrame(D, columns=["tabla", "campo", "significado_negocio", "origen", "regla_transformacion",
                                       "unidad"])
modelo = {"fact_venta_linea": fact_venta_linea, "dim_fecha": dim_fecha, "dim_cliente": dim_cliente,
          "dim_tienda": dim_tienda, "dim_producto": dim_producto, "dim_campana": dim_campana}
en_modelo = {(t, c) for t, df in modelo.items() for c in df.columns}
en_dicc = set(zip(diccionario.tabla, diccionario.campo))
pruebas.verificar("O-DIC-01", "Diccionario", "Todo campo del modelo está en el diccionario y viceversa",
                  en_modelo == en_dicc, len(en_modelo ^ en_dicc))

# COMMAND ----------

# MAGIC %md ## Publicación

# COMMAND ----------

pruebas.cerrar()
for t, df in modelo.items():
    escribir_pandas(df, "oro", t)
escribir_pandas(diccionario, "oro", "diccionario_datos")
bit.guardar()
