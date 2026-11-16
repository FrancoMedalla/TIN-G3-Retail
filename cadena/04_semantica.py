# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Capa semántica y consumo
# MAGIC - `oro.cliente_ventana`: una fila por cliente y ventana (30, 90 y 180 días) con su estado entre los cortes del
# MAGIC   30-jun y el 31-dic: retenido, perdido, ganado nuevo o ganado reactivado.
# MAGIC - `oro.mv_retencion`: metric view con la definición de las medidas (`METRICAS` en `comun.py`).
# MAGIC - Tablas de consumo del informe y del tablero, todas consultadas desde la metric view.
# MAGIC - Evidencia complementaria, caso de negocio en margen, estado de la corrida y copia de las cifras en CSV.

# COMMAND ----------

import numpy as np
import pandas as pd

from comun import (CIFRAS_PUBLICADAS, CORTE_1, CORTE_2, EN_DATABRICKS, ID_CORRIDA, METRICAS, REGLA_LECTURA,
                   UMBRAL_LECTURA, VENTANA_BASE, VENTANAS, Bitacora, Pruebas, a_pandas, ahora, escribir_pandas,
                   guardar_salida_csv, leer, nombre_tabla, spark, sql_metricas, yaml_metric_view)

bit = Bitacora("semantica")
registrar = bit.registrar
pruebas = Pruebas("semantica")
C1, C2 = pd.Timestamp(CORTE_1), pd.Timestamp(CORTE_2)

fact = a_pandas("oro", "fact_venta_linea")
dim_cliente, dim_tienda = a_pandas("oro", "dim_cliente"), a_pandas("oro", "dim_tienda")
dim_fecha = a_pandas("oro", "dim_fecha")
cobertura = a_pandas("control", "cobertura").sort_values("orden").reset_index(drop=True)
n_origen = int(cobertura.loc[cobertura.paso == "Boletas en origen", "boletas"].iloc[0])
n_usadas = int(cobertura.loc[cobertura.paso.str.startswith("= Visitas"), "boletas"].iloc[0])

# COMMAND ----------

# MAGIC %md ## Visitas: boleta con monto neto > 0 (regla B07), solo clientes identificados

# COMMAND ----------


def visitas_por_boleta(hechos):
    b = (hechos.groupby(["boleta_id", "fecha_key", "cliente_key", "tienda_key"], as_index=False)
               .agg(monto_boleta=("monto_linea", "sum"), monto_boleta_real_dic2025=("monto_linea_real_dic2025", "sum"),
                    margen_boleta_real_dic2025=("margen_linea_real_dic2025", "sum")))
    b["es_visita"] = b.monto_boleta > 0
    b["es_anonima"] = b.cliente_key.eq(0)
    return b


vis = visitas_por_boleta(fact)
vis = vis[vis.es_visita & ~vis.es_anonima].copy()
vis["fecha"] = pd.to_datetime(vis.fecha_key.astype(str), format="%Y%m%d")
vis = vis.merge(dim_cliente[["cliente_key", "tipo_cliente", "primera_visita_2025"]], on="cliente_key", how="left")
pruebas.verificar("S-VIS-01", "Métrica", "Visitas identificadas = cobertura", len(vis) == n_usadas,
                  abs(len(vis) - n_usadas))

tipo_de = dim_cliente.set_index("cliente_key").tipo_cliente
primera_de = pd.to_datetime(dim_cliente.set_index("cliente_key").primera_visita_2025)
segmento_de = dim_cliente.set_index("cliente_key").segmento
alta_post = (pd.to_datetime(dim_cliente.fecha_alta) > pd.to_datetime(dim_cliente.primera_visita_2025))
alta_post_de = pd.Series(alta_post.astype(int).values, index=dim_cliente.cliente_key)

# COMMAND ----------

# MAGIC %md ## Estado de cada cliente por ventana
# MAGIC Activo al corte C con ventana W: al menos una visita entre C − (W − 1) días y C, inclusive.

# COMMAND ----------


def activos(corte, w):
    desde = corte - pd.Timedelta(days=w - 1)
    return set(vis[(vis.fecha >= desde) & (vis.fecha <= corte)].cliente_key)


def tienda_principal(w):
    desde = C1 - pd.Timedelta(days=w - 1)
    v = vis[(vis.fecha >= desde) & (vis.fecha <= C1)].merge(dim_tienda[["tienda_key", "tienda", "canal"]], on="tienda_key")
    return v.groupby("cliente_key").tienda.agg(lambda x: x.value_counts().index[0])


filas = []
for w in VENTANAS:
    base, final = activos(C1, w), activos(C2, w)
    tp = tienda_principal(w)
    for k in sorted(base | final):
        jun, dic = int(k in base), int(k in final)
        ganado = int(dic and not jun)
        nuevo = int(ganado and tipo_de[k] == "Nuevo 2025" and primera_de[k] > C1)
        filas.append({"cliente_key": int(k), "ventana_dias": w, "activo_jun": jun, "activo_dic": dic,
                      "retenido": int(jun and dic), "perdido": int(jun and not dic), "ganado": ganado,
                      "ganado_nuevo": nuevo, "ganado_reactivado": ganado - nuevo,
                      "tipo_cliente": tipo_de[k], "segmento": segmento_de[k],
                      "tienda_principal": tp.get(k) if jun else None,
                      "alta_posterior_a_primera_compra": int(alta_post_de[k])})
cliente_ventana = pd.DataFrame(filas)

# COMMAND ----------

# MAGIC %md ## Metric view

# COMMAND ----------

escribir_pandas(cliente_ventana, "oro", "cliente_ventana")
fuente_mv = nombre_tabla("oro", "cliente_ventana") if EN_DATABRICKS else "cliente_ventana"
usa_metric_view = False
if EN_DATABRICKS:
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {nombre_tabla('oro', 'mv_retencion')} WITH METRICS LANGUAGE YAML AS $$\n"
                  f"{yaml_metric_view(fuente_mv)}\n$$")
        usa_metric_view = True
        print("Metric view creada:", nombre_tabla("oro", "mv_retencion"))
    except Exception as e:
        print("No se pudo crear la metric view; se usa la consulta SQL equivalente generada desde METRICAS:", str(e)[:300])
else:
    leer("oro", "cliente_ventana").createOrReplaceTempView("cliente_ventana")


def consultar(dimensiones=("ventana_dias",), filtro=None):
    """Consulta las medidas en la metric view o, si no está disponible, en la consulta SQL equivalente."""
    dims = list(dimensiones)
    if usa_metric_view:
        medidas = ", ".join(f"MEASURE({m}) AS {m}" for m in METRICAS)
        donde = f"WHERE {filtro}" if filtro else ""
        q = (f"SELECT {', '.join(dims)}, {medidas} FROM {nombre_tabla('oro', 'mv_retencion')} {donde} "
             f"GROUP BY {', '.join(dims)} ORDER BY {', '.join(dims)}")
    else:
        q = sql_metricas(fuente_mv, dims, filtro)
    return spark.sql(q).toPandas()


metrica = consultar(["ventana_dias"])
metrica["conclusion"] = np.where(metrica.tasa_reposicion > UMBRAL_LECTURA, "Repone", "Retiene")
metrica["regla_de_lectura"] = REGLA_LECTURA
metrica["es_definicion_base"] = metrica.ventana_dias.eq(VENTANA_BASE)
print(metrica[["ventana_dias", "activos_jun", "retenidos", "tasa_retencion", "tasa_reposicion", "conclusion"]])

# COMMAND ----------

# MAGIC %md ## Cortes: tipo de cliente, sensibilidad K05, segmento y local (con intervalo de confianza)

# COMMAND ----------


def wilson(k, n, z=1.96):
    if n == 0:
        return np.nan, np.nan
    p = k / n
    c = (p + z * z / (2 * n)) / (1 + z * z / n)
    h = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return c - h, c + h


por_tipo = consultar(["ventana_dias", "tipo_cliente"])
por_tipo_sin311 = consultar(["ventana_dias", "tipo_cliente"], "alta_posterior_a_primera_compra = 0")
sens = (por_tipo[["ventana_dias", "tipo_cliente", "activos_jun", "tasa_retencion"]]
        .merge(por_tipo_sin311[["ventana_dias", "tipo_cliente", "activos_jun", "tasa_retencion"]],
               on=["ventana_dias", "tipo_cliente"], suffixes=("_todos", "_sin_alta_posterior")))
por_segmento = consultar(["ventana_dias", "segmento"])
por_tienda = consultar(["ventana_dias", "tienda_principal"], "tienda_principal IS NOT NULL")
total = metrica.set_index("ventana_dias").tasa_retencion
ic = por_tienda.apply(lambda r: wilson(r.retenidos, r.activos_jun), axis=1)
por_tienda["ic95_inferior"], por_tienda["ic95_superior"] = ic.str[0], ic.str[1]
por_tienda["tasa_cadena"] = por_tienda.ventana_dias.map(total)
por_tienda["lectura"] = np.where(por_tienda.ic95_superior < por_tienda.tasa_cadena, "Bajo la cadena",
                                 np.where(por_tienda.ic95_inferior > por_tienda.tasa_cadena, "Sobre la cadena",
                                          "No concluyente"))

h, n = por_tipo.query("ventana_dias == 90").set_index("tipo_cliente"), sens.query("ventana_dias == 90").set_index("tipo_cliente")
brecha_todos = h.loc["Nuevo 2025", "tasa_retencion"] - h.loc["Base heredada", "tasa_retencion"]
brecha_k05 = (n.loc["Nuevo 2025", "tasa_retencion_sin_alta_posterior"]
              - n.loc["Base heredada", "tasa_retencion_sin_alta_posterior"])
registrar("K05", "métrica", "Sensibilidad de la antigüedad: retención por tipo de cliente con y sin los clientes con "
          "alta posterior a su primera compra",
          f"La brecha nuevos - heredada pasa de {brecha_todos * 100:.1f} a {brecha_k05 * 100:.1f} "
          "puntos; la base heredada no cambia porque esos clientes tienen el alta en 2025",
          int(alta_post.sum()), "La brecha por antigüedad no es concluyente; la conclusión de la P1 (repone) no cambia",
          "medido")
registrar("K06", "métrica", "Locales: una tasa se separa de la cadena solo si su intervalo de confianza al 95% "
          "(Wilson) no contiene la tasa de la cadena",
          "Con 33 a 46 clientes por local el intervalo es de unos ±13 a 16 puntos; una diferencia menor puede ser "
          "azar", int((por_tienda.ventana_dias == 90).sum()),
          "Si se usara la diferencia simple, se recomendarían locales cuya diferencia puede ser azar", "medido")

# COMMAND ----------

# MAGIC %md ## Evidencia complementaria

# COMMAND ----------

nv = vis[vis.tipo_cliente == "Nuevo 2025"].copy()
nv["primera"] = nv.cliente_key.map(nv.groupby("cliente_key").fecha.min())
nv["dias"] = (nv.fecha - nv.primera).dt.days
segunda = nv[nv.dias > 0].groupby("cliente_key").dias.min()
coh = nv.groupby("cliente_key").primera.min().to_frame()
coh = coh[coh.primera.dt.month <= 9]  # solo meses de llegada con 90 días completos de observación
coh["vuelve_90"] = coh.index.map(lambda k: segunda.get(k, np.inf) <= 90)
coh["mes_llegada"] = coh.primera.dt.month
recompra = (coh.groupby("mes_llegada").agg(clientes_nuevos=("vuelve_90", "size"), vuelven_90=("vuelve_90", "sum"))
            .reset_index())
recompra["tasa_recompra_90"] = recompra.vuelven_90 / recompra.clientes_nuevos

vm = vis.merge(dim_fecha[["fecha_key", "mes"]], on="fecha_key", how="left")
mensual = (vm.groupby(["mes", "tipo_cliente"]).agg(clientes=("cliente_key", "nunique"),
                                                    venta_real_dic2025=("monto_boleta_real_dic2025", "sum"))
           .reset_index())

gaps = vis.sort_values(["cliente_key", "fecha"]).groupby("cliente_key").fecha.diff().dt.days.dropna()
pausas = pd.DataFrame([{"pausas": len(gaps), "mediana_dias": gaps.median(), "p75_dias": gaps.quantile(.75),
                        "pct_mayor_30": (gaps > 30).mean(), "pct_mayor_45": (gaps > 45).mean(),
                        "pct_mayor_90": (gaps > 90).mean()}])

# COMMAND ----------

# MAGIC %md ## Caso de negocio en margen (pesos de diciembre de 2025)

# COMMAND ----------

base90 = cliente_ventana.query("ventana_dias == 90")
perdidos = set(base90.query("perdido == 1").cliente_key)
activos_jun = int(base90.activo_jun.sum())
h1 = vis[vis.fecha <= C1]
venta_perd = h1[h1.cliente_key.isin(perdidos)].monto_boleta_real_dic2025.sum()
margen_perd = h1[h1.cliente_key.isin(perdidos)].margen_boleta_real_dic2025.sum()
venta_h1 = h1.monto_boleta_real_dic2025.sum()
por_10pp = round(0.10 * activos_jun)
mensual_venta, mensual_margen = venta_perd / len(perdidos) / 6, margen_perd / len(perdidos) / 6
caso = pd.DataFrame([
    ("clientes_perdidos_90_dias", len(perdidos), "clientes"),
    ("venta_1er_semestre_perdidos", round(venta_perd), "$ dic-2025"),
    ("margen_1er_semestre_perdidos", round(margen_perd), "$ dic-2025"),
    ("pct_venta_identificada_1er_semestre", venta_perd / venta_h1, "proporción"),
    ("venta_mensual_por_cliente_perdido", round(mensual_venta), "$ dic-2025"),
    ("margen_mensual_por_cliente_perdido", round(mensual_margen), "$ dic-2025"),
    ("clientes_por_10_puntos_de_retencion", por_10pp, "clientes"),
    ("venta_anual_por_10_puntos", round(por_10pp * mensual_venta * 12), "$ dic-2025"),
    ("margen_anual_por_10_puntos", round(por_10pp * mensual_margen * 12), "$ dic-2025"),
], columns=["concepto", "valor", "unidad"])
print(caso)

# COMMAND ----------

# MAGIC %md ## Reglas de la métrica (bitácora)

# COMMAND ----------

res = metrica.set_index("ventana_dias")
misma = res.loc[90, "conclusion"] == res.loc[180, "conclusion"]
registrar("K01", "métrica", "Ventana de actividad de 90 días (alternativa 180; referencia 30)",
          f"Mediana entre compras {gaps.median():.0f} días; {(gaps > 90).mean():.1%} de las pausas superan 90 días "
          f"y {(gaps > 30).mean():.1%} superan 30", int(res.loc[90, "activos_jun"]),
          f"Con 180 días la retención sube a {res.loc[180, 'tasa_retencion']:.1%}; la conclusión "
          f"{'se mantiene' if misma else 'cambia'}", "verbalizado (creencia de 30 días) y medido")
registrar("K02", "métrica", "Cortes al 30-jun y al 31-dic para que las ventanas no se traslapen",
          "El 30-jun es el primer cierre de mes con 180 días de historia y el 31-dic es el último dato; que coincidan "
          "con el fin de cada semestre no es un criterio", 2,
          "Diciembre es temporada alta: clientes que solo compran en fiestas pueden aparecer como activos", "inferido")
vt = vis.merge(dim_fecha[["fecha_key", "trimestre"]], on="fecha_key")
t2, t4 = set(vt[vt.trimestre == "T2"].cliente_key), set(vt[vt.trimestre == "T4"].cliente_key)
ret_trim, rep_trim = len(t2 & t4) / len(t2), len(t4 - t2) / len(t4)
registrar("K04", "métrica", "Ventana móvil de 90 días y no trimestre calendario (prueba de robustez: T2 frente a T4)",
          f"Con trimestres calendario (dim_fecha.trimestre) la retención es {ret_trim:.1%} y la reposición "
          f"{rep_trim:.1%}", len(t2),
          f"Ninguno: la retención cambia {abs(ret_trim - res.loc[90, 'tasa_retencion']) * 100:.1f} puntos y la "
          f"conclusión ({'repone' if rep_trim > UMBRAL_LECTURA else 'retiene'}) se mantiene", "medido")
registrar("K03", "métrica", "Tienda principal = tienda con más visitas del cliente en la ventana de junio",
          "La mayoría de los clientes compra en más de un local; se asigna uno para cortar la retención",
          int(res.loc[90, "activos_jun"]), "Un cliente repartido entre locales queda asignado solo a uno", "inferido")

# COMMAND ----------

# MAGIC %md ## Pruebas de la métrica

# COMMAND ----------

pruebas.verificar("S-MET-01", "Métrica", "Tasas entre 0 y 1",
                  metrica.tasa_retencion.between(0, 1).all() and metrica.tasa_reposicion.between(0, 1).all())
cuadra = (metrica.activos_dic == metrica.retenidos + metrica.ganados).all() and \
         (metrica.activos_jun == metrica.retenidos + metrica.perdidos).all() and \
         (metrica.ganados == metrica.ganados_nuevos + metrica.ganados_reactivados).all()
pruebas.verificar("S-MET-02", "Métrica", "La cascada cierra: jun = retenidos + perdidos; dic = retenidos + ganados",
                  cuadra)
pruebas.verificar("S-MET-03", "Métrica", "Cada cliente aparece una vez por ventana",
                  not cliente_ventana.duplicated(["cliente_key", "ventana_dias"]).any())
pruebas.verificar("S-MET-04", "Métrica", "El corte por local suma el total de activos al 30-jun",
                  (por_tienda.groupby("ventana_dias").activos_jun.sum() == res.activos_jun).all())
dif = max(abs(res.loc[w, "tasa_retencion"] - r) + abs(res.loc[w, "tasa_reposicion"] - p)
          for w, (r, p) in CIFRAS_PUBLICADAS.items())
pruebas.verificar("S-MET-05", "Métrica", "La cadena reproduce las cifras publicadas en el informe (±0,1 punto)",
                  dif < 0.001, severidad="avisa",
                  accion="Seguir; el informe debe explicar el cambio en el registro de mejora")

# COMMAND ----------

# MAGIC %md ## Publicación

# COMMAND ----------

pruebas.cerrar()
for tabla, df in {"metrica_retencion": metrica, "retencion_por_tipo": por_tipo, "sensibilidad_k05": sens,
                  "retencion_por_segmento": por_segmento, "retencion_por_tienda": por_tienda,
                  "evidencia_recompra_nuevos": recompra, "evidencia_clientes_mes": mensual,
                  "pausas_entre_compras": pausas, "caso_negocio": caso}.items():
    escribir_pandas(df, "oro", tabla)
bit.guardar()

bitacora = pd.concat([a_pandas("control", f"bitacora_{c}") for c in ("plata", "oro", "semantica")], ignore_index=True)
escribir_pandas(bitacora, "control", "bitacora_supuestos")
todas = a_pandas("control", "pruebas_resultados")
ultima = todas[todas.id_corrida == ID_CORRIDA]
carga = a_pandas("control", "bitacora_carga")
origen_ipc = carga[carga.id_corrida == ID_CORRIDA].origen_ipc.iloc[0] if (carga.id_corrida == ID_CORRIDA).any() else "?"
estado = pd.DataFrame([{
    "id_corrida": ID_CORRIDA, "fecha_hora": ahora(), "origen_ipc": origen_ipc,
    "pruebas": len(ultima), "pruebas_ok": int((ultima.resultado == "pasó").sum()),
    "avisos": int(((ultima.resultado == "falló") & (ultima.severidad == "avisa")).sum()),
    "pct_pruebas_ok": (ultima.resultado == "pasó").mean(),
    "cobertura_boletas": n_usadas / n_origen,
    "fuente": "POS, maestro de productos, campañas y IPC (mindicador.cl) de Multitienda del Sur, 2025",
    "dueno_dato": "Gerencia Comercial",
    "regla_de_lectura": REGLA_LECTURA}])
escribir_pandas(estado, "control", "estado_corrida", modo="append")

guardar_salida_csv(metrica.drop(columns="regla_de_lectura"), "metrica_retencion.csv", index=False)
guardar_salida_csv(por_tipo, "retencion_por_tipo.csv", index=False)
guardar_salida_csv(sens, "sensibilidad_k05.csv", index=False)
guardar_salida_csv(por_tienda, "retencion_por_tienda.csv", index=False)
guardar_salida_csv(por_segmento, "retencion_por_segmento.csv", index=False)
guardar_salida_csv(recompra, "recompra_90_nuevos_por_mes.csv", index=False)
guardar_salida_csv(mensual, "clientes_y_venta_mensual_por_tipo.csv", index=False)
guardar_salida_csv(caso, "caso_de_negocio.csv", index=False)
guardar_salida_csv(cobertura, "cobertura.csv", index=False)
guardar_salida_csv(pausas, "pausas_entre_compras.csv", index=False)
guardar_salida_csv(bitacora, "bitacora_supuestos.csv", index=False)
guardar_salida_csv(ultima, "pruebas_ultima_corrida.csv", index=False)
guardar_salida_csv(a_pandas("oro", "diccionario_datos"), "diccionario_datos.csv", index=False)
print(f"Corrida {ID_CORRIDA} publicada: {estado.pruebas_ok.iloc[0]}/{estado.pruebas.iloc[0]} pruebas en verde, "
      f"IPC desde {origen_ipc}.")
