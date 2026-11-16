"""Genera tablero/retencion_clientes.lvdash.json (dashboard AI/BI de Databricks).

    python tablero/generar_tablero.py [catalogo]
"""
import json
import os
import sys

CAT = sys.argv[1] if len(sys.argv) > 1 else "workspace"
MV = f"{CAT}.oro.mv_retencion"
SALIDA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "retencion_clientes.lvdash.json")

NAVY, NARANJO, NARANJO_SUAVE, GRIS, GRIS_CLARO = "#14213D", "#E8612C", "#F4B496", "#8C8A84", "#D9D6CF"
MEDIDAS = ["tasa_retencion", "tasa_reposicion", "activos_jun", "retenidos", "perdidos", "ganados",
           "ganados_nuevos", "ganados_reactivados", "activos_dic"]
FILTRO = "ventana_dias = CAST(:ventana AS INT) AND (:tipo = 'Todos' OR tipo_cliente = :tipo)"


def num(expr, dec=1):
    """Número con coma decimal y punto de miles."""
    return f"translate(format_number({expr}, {dec}), ',.', '.,')"


def param(keyword, defecto):
    return {"displayName": keyword, "keyword": keyword, "dataType": "STRING",
            "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": defecto}]}}}


P_VENTANA, P_TIPO = param("ventana", "90"), param("tipo", "Todos")
P_COSTO, P_CUPON = param("costo_contacto", "100"), param("cupon", "10")

DATASETS = {
    "opciones_ventana": ("Opciones de ventana", "SELECT explode(array('30', '90', '180')) AS ventana", []),
    "opciones_tipo": ("Opciones de tipo de cliente",
                      "SELECT explode(array('Todos', 'Base heredada', 'Nuevo 2025')) AS tipo_cliente", []),
    "opciones_costo": ("Opciones de costo por contacto",
                       "SELECT explode(array('50', '100', '150', '200', '300', '500')) AS costo_contacto", []),
    "opciones_cupon": ("Opciones de cupón", "SELECT explode(array('0', '5', '10', '15', '20')) AS cupon", []),
    "kpi": ("Cifras clave", f"""
WITH m AS (
  SELECT {', '.join(f'MEASURE({x}) AS {x}' for x in MEDIDAS)}
  FROM {MV}
  WHERE {FILTRO}
),
cob AS (
  SELECT MAX(CASE WHEN paso = 'Boletas en origen' THEN boletas END) AS origen,
         MAX(CASE WHEN paso LIKE '= Visitas%' THEN boletas END) AS usadas
  FROM {CAT}.control.cobertura
)
SELECT m.*,
  CASE WHEN tasa_reposicion > 0.5 THEN 'REPONE' ELSE 'RETIENE' END AS conclusion,
  concat('Meta propuesta: ', {num('(tasa_retencion + 0.10) * 100')}, '% (+10 pts, ', CAST(perdidos AS STRING),
         ' clientes dejaron de comprar)') AS comparacion_retencion,
  concat(CASE WHEN tasa_reposicion > 0.5 THEN 'Sobre' ELSE 'Bajo' END,
         ' el umbral de 50%: ', CAST(ganados AS STRING), ' de ', CAST(activos_dic AS STRING),
         ' activos en diciembre no estaban en junio') AS comparacion_reposicion,
  concat('La cadena ', CASE WHEN tasa_reposicion > 0.5 THEN 'REPONE' ELSE 'RETIENE' END,
         ' clientes') AS conclusion_texto,
  'Regla fijada antes de calcular: repone si más del 50% de los activos al 31-dic no estaba activo al 30-jun'
    AS regla_lectura,
  concat('Cobertura: ', {num('usadas / origen * 100')}, '% de las boletas (', {num('usadas', 0)}, ' de ',
         {num('origen', 0)}, '); las anónimas no entran') AS cobertura_texto
FROM m CROSS JOIN cob
""", [P_VENTANA, P_TIPO]),
    "cascada": ("Base activa junio y diciembre", f"""
WITH m AS (
  SELECT MEASURE(retenidos) AS r, MEASURE(perdidos) AS p, MEASURE(ganados_nuevos) AS n,
         MEASURE(ganados_reactivados) AS a
  FROM {MV}
  WHERE {FILTRO}
)
SELECT 'Activos al 30-jun' AS corte, 'Retenidos' AS estado, r AS clientes FROM m
UNION ALL SELECT 'Activos al 30-jun', 'Dejaron de comprar', p FROM m
UNION ALL SELECT 'Activos al 31-dic', 'Retenidos', r FROM m
UNION ALL SELECT 'Activos al 31-dic', 'Nuevos', n FROM m
UNION ALL SELECT 'Activos al 31-dic', 'Volvieron', a FROM m
""", [P_VENTANA, P_TIPO]),
    "tiendas": ("Retención por local", f"""
WITH t AS (
  SELECT tienda_principal AS tienda, MEASURE(retenidos) AS retenidos, MEASURE(activos_jun) AS activos_jun
  FROM {MV}
  WHERE {FILTRO} AND tienda_principal IS NOT NULL
  GROUP BY tienda_principal
),
c AS (
  SELECT MEASURE(tasa_retencion) AS tasa_cadena FROM {MV} WHERE {FILTRO}
),
w AS (
  SELECT tienda, activos_jun, retenidos, retenidos / activos_jun AS tasa,
    ((retenidos / activos_jun) + 1.96 * 1.96 / (2 * activos_jun)
      - 1.96 * sqrt((retenidos / activos_jun) * (1 - retenidos / activos_jun) / activos_jun
                    + 1.96 * 1.96 / (4 * activos_jun * activos_jun)))
      / (1 + 1.96 * 1.96 / activos_jun) AS ic_inferior,
    ((retenidos / activos_jun) + 1.96 * 1.96 / (2 * activos_jun)
      + 1.96 * sqrt((retenidos / activos_jun) * (1 - retenidos / activos_jun) / activos_jun
                    + 1.96 * 1.96 / (4 * activos_jun * activos_jun)))
      / (1 + 1.96 * 1.96 / activos_jun) AS ic_superior
  FROM t
)
SELECT w.tienda, w.activos_jun, w.retenidos, w.tasa, w.ic_inferior, w.ic_superior, c.tasa_cadena,
  CASE WHEN w.ic_superior < c.tasa_cadena THEN 'Bajo la cadena'
       WHEN w.ic_inferior > c.tasa_cadena THEN 'Sobre la cadena'
       ELSE 'No concluyente' END AS lectura,
  concat(CAST(round(w.ic_inferior * 100) AS INT), '% a ', CAST(round(w.ic_superior * 100) AS INT), '%')
    AS intervalo_95
FROM w CROSS JOIN c
ORDER BY w.tienda
""", [P_VENTANA, P_TIPO]),
    "vale_cuesta": ("Valor y costo de la acción", f"""
WITH x AS (
  SELECT c.*, CAST(:costo_contacto AS DOUBLE) AS costo, CAST(:cupon AS DOUBLE) / 100 AS cup
  FROM {CAT}.oro.contacto_34_45 c
),
y AS (
  SELECT x.*,
    contactos_al_anio * costo
      + (vuelven_solos_antes_de_45 + vuelven_solos_entre_46_y_90) * venta_media_por_visita * cup AS costo_anual
  FROM x
)
SELECT
  concat('$', {num('margen_anual_por_10_puntos / 1000000', 2)}, ' MM') AS vale_texto,
  concat('$', {num('costo_anual / 1000000', 2)}, ' MM') AS cuesta_texto,
  concat(CAST(CEIL(costo_anual / margen_anual_por_cliente_recuperado) AS STRING), ' de ',
         CAST(en_riesgo_no_vuelven_en_90 AS STRING), ' clientes en riesgo (',
         {num('CEIL(costo_anual / margen_anual_por_cliente_recuperado) / en_riesgo_no_vuelven_en_90 * 100')}, '%)')
    AS equilibrio_texto,
  concat({num('contactos_al_anio', 0)}, ' contactos al año (clientes que cumplen 34 días sin comprar) a $',
         {num('costo', 0)}, ' cada uno, más un cupón de ', CAST(CAST(cup * 100 AS INT) AS STRING),
         '% sobre la compra de quienes vuelven. Cada cliente recuperado deja $',
         {num('margen_anual_por_cliente_recuperado', 0)}, ' de margen al año.') AS supuestos_texto
FROM y
""", [P_COSTO, P_CUPON]),
    "estado": ("Estado de la última corrida", f"""
SELECT concat('Fuente: ', fuente, '. Actualizado: ', fecha_hora, ' (corrida ', id_corrida, '). Dueño del dato: ',
              dueno_dato, '. Pruebas de calidad: ', CAST(pruebas_ok AS STRING), ' de ', CAST(pruebas AS STRING),
              ' en verde. IPC desde ',
              CASE WHEN origen_ipc = 'api' THEN 'la API de mindicador.cl'
                   ELSE 'el respaldo (la API no respondió)' END, '.') AS pie
FROM {CAT}.control.estado_corrida
ORDER BY fecha_hora DESC
LIMIT 1
""", []),
    "ventanas": ("Retención y reposición por ventana", f"""
WITH m AS (
  SELECT ventana_dias, MEASURE(tasa_retencion) AS r, MEASURE(tasa_reposicion) AS p
  FROM {MV}
  GROUP BY ventana_dias
),
e AS (
  SELECT ventana_dias, r, p,
    concat(CAST(ventana_dias AS STRING), ' días',
           CASE ventana_dias WHEN 90 THEN ' (definición)' WHEN 180 THEN ' (alternativa)'
                ELSE ' (referencia de la contraparte)' END) AS ventana
  FROM m
)
SELECT ventana, ventana_dias, 'Retención' AS medida, r AS valor FROM e
UNION ALL SELECT ventana, ventana_dias, 'Reposición', p FROM e
""", []),
    "ventanas_tabla": ("Detalle por ventana", f"""
SELECT
  concat(CAST(ventana_dias AS STRING), ' días') AS ventana,
  concat({num('MEASURE(tasa_retencion) * 100')}, '% (', CAST(MEASURE(retenidos) AS STRING), ' de ',
         CAST(MEASURE(activos_jun) AS STRING), ')') AS retencion,
  concat({num('MEASURE(tasa_reposicion) * 100')}, '% (', CAST(MEASURE(ganados) AS STRING), ' de ',
         CAST(MEASURE(activos_dic) AS STRING), ')') AS reposicion,
  CASE WHEN MEASURE(tasa_reposicion) > 0.5 THEN 'Repone' ELSE 'Retiene' END AS lectura
FROM {MV}
GROUP BY ventana_dias
ORDER BY ventana_dias
""", []),
    "antiguedad": ("Retención por antigüedad: sensibilidad", f"""
SELECT 'Todos los clientes' AS grupo, tipo_cliente, MEASURE(tasa_retencion) AS tasa,
       MEASURE(activos_jun) AS activos_jun
FROM {MV}
WHERE ventana_dias = 90
GROUP BY tipo_cliente
UNION ALL
SELECT 'Sin clientes con alta posterior a su primera compra', tipo_cliente, MEASURE(tasa_retencion),
       MEASURE(activos_jun)
FROM {MV}
WHERE ventana_dias = 90 AND alta_posterior_a_primera_compra = 0
GROUP BY tipo_cliente
""", []),
}


def dataset(nombre):
    titulo, sql, params = DATASETS[nombre]
    lineas = [ln + "\n" for ln in sql.strip().split("\n")]
    d = {"name": nombre, "displayName": titulo, "queryLines": lineas}
    if params:
        d["parameters"] = params
    return d


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def texto(nombre, lineas, p):
    return {"widget": {"name": nombre, "multilineTextboxSpec": {"lines": [ln + "\n" for ln in lineas]}}, "position": p}


def contador(nombre, ds, campo, titulo, p, formato=None, tam=None, descripcion=None):
    enc = {"fieldName": campo, "displayName": titulo}
    if formato:
        enc["format"] = formato
    if tam:
        enc["style"] = {"fontSize": tam}
    frame = {"showTitle": True, "title": titulo}
    if descripcion:
        frame.update(showDescription=True, description=descripcion)
    return {"widget": {"name": nombre,
                       "queries": [{"name": "main_query", "query": {
                           "datasetName": ds, "fields": [{"name": campo, "expression": f"`{campo}`"}],
                           "disaggregated": True}}],
                       "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": enc}, "frame": frame}},
            "position": p}


def filtro(nombre, titulo, opciones, campo, parametro, datasets, defecto, p):
    queries = [{"name": f"{nombre}_opciones", "query": {
        "datasetName": opciones, "fields": [{"name": campo, "expression": f"`{campo}`"}], "disaggregated": False}}]
    campos = [{"fieldName": campo, "displayName": titulo, "queryName": f"{nombre}_opciones"}]
    for ds in datasets:
        q = f"{nombre}_{ds}"
        queries.append({"name": q, "query": {"datasetName": ds,
                                             "parameters": [{"name": parametro, "keyword": parametro}],
                                             "disaggregated": False}})
        campos.append({"parameterName": parametro, "queryName": q})
    return {"widget": {"name": nombre, "queries": queries,
                       "spec": {"version": 2, "widgetType": "filter-single-select",
                                "encodings": {"fields": campos},
                                "selection": {"defaultSelection": {"values": {
                                    "dataType": "STRING", "values": [{"value": defecto}]}}},
                                "frame": {"showTitle": True, "title": titulo}}},
            "position": p}


def barras(nombre, ds, x, y, color, titulo, p, mapeo=None, horizontal=False, agrupadas=False, formato_y=None,
           titulo_x=None, titulo_y=None):
    campos = [{"name": x, "expression": f"`{x}`"}, {"name": color, "expression": f"`{color}`"},
              {"name": f"sum({y})", "expression": f"SUM(`{y}`)"}]
    if color == x:
        campos = campos[:1] + campos[2:]
    eje_cat = {"fieldName": x, "scale": {"type": "categorical"}, "displayName": titulo_x or x}
    eje_val = {"fieldName": f"sum({y})", "scale": {"type": "quantitative"}, "displayName": titulo_y or y}
    if formato_y:
        eje_val["format"] = formato_y
    col = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    if mapeo:
        col["scale"]["mappings"] = [{"value": k, "color": v} for k, v in mapeo.items()]
    enc = {"x": eje_val, "y": eje_cat} if horizontal else {"x": eje_cat, "y": eje_val}
    enc["color"] = col
    spec = {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": titulo},
            "mark": {"layout": "group" if agrupadas else "stack"}}
    return {"widget": {"name": nombre, "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": campos, "disaggregated": False}}], "spec": spec}, "position": p}


def tabla(nombre, ds, columnas, titulo, p):
    return {"widget": {"name": nombre, "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": [{"name": c, "expression": f"`{c}`"} for c, _ in columnas],
        "disaggregated": True}}],
        "spec": {"version": 1, "widgetType": "table",
                 "encodings": {"columns": [{"fieldName": c, "displayName": t, "title": t, "visible": True,
                                            "order": i, "type": "string", "displayAs": "string"}
                                           for i, (c, t) in enumerate(columnas)]},
                 "frame": {"showTitle": True, "title": titulo}}}, "position": p}


PCT = {"type": "number-percent", "decimalPlaces": {"type": "fixed", "places": 1}}
PARAM_PRINCIPAL = ["kpi", "cascada", "tiendas"]

pagina_principal = [
    texto("titulo", ["## ¿La cadena está reteniendo a sus clientes o los está reponiendo?",
                     "Clientes identificados de Multitienda del Sur · cortes al 30-jun y al 31-dic de 2025 · "
                     "un cliente está activo si compró en la ventana elegida antes del corte"], pos(0, 0, 6, 2)),
    filtro("filtro_ventana", "Ventana de cliente activo (días)", "opciones_ventana", "ventana", "ventana",
           PARAM_PRINCIPAL, "90", pos(0, 2, 2, 1)),
    filtro("filtro_tipo", "Tipo de cliente", "opciones_tipo", "tipo_cliente", "tipo", PARAM_PRINCIPAL, "Todos",
           pos(2, 2, 2, 1)),
    contador("regla", "kpi", "regla_lectura", "Regla de lectura", pos(4, 2, 2, 1), tam=12),
    contador("kpi_retencion", "kpi", "tasa_retencion", "Retención semestral", pos(0, 3, 2, 3), formato=PCT),
    contador("kpi_reposicion", "kpi", "tasa_reposicion", "Reposición: activos de diciembre que son nuevos o volvieron",
             pos(2, 3, 2, 3), formato=PCT),
    contador("kpi_conclusion", "kpi", "conclusion_texto", "Respuesta", pos(4, 3, 2, 3)),
    contador("cmp_retencion", "kpi", "comparacion_retencion", "Comparación", pos(0, 6, 2, 1), tam=13),
    contador("cmp_reposicion", "kpi", "comparacion_reposicion", "Comparación", pos(2, 6, 2, 1), tam=13),
    contador("cobertura", "kpi", "cobertura_texto", "Cobertura", pos(4, 6, 2, 1), tam=13),
    barras("grafico_cascada", "cascada", "corte", "clientes", "estado",
           "Clientes activos al 30-jun y al 31-dic (n.º de clientes)", pos(0, 7, 3, 6),
           mapeo={"Retenidos": NAVY, "Dejaron de comprar": GRIS_CLARO, "Nuevos": NARANJO, "Volvieron": NARANJO_SUAVE},
           titulo_x="Corte", titulo_y="Clientes"),
    barras("grafico_locales", "tiendas", "tienda", "tasa", "lectura",
           "Retención por local principal (% de clientes); en naranjo, el que se separa de la cadena",
           pos(3, 7, 3, 6), mapeo={"Bajo la cadena": NARANJO, "No concluyente": GRIS_CLARO, "Sobre la cadena": NAVY},
           horizontal=True, formato_y=PCT, titulo_x="Local", titulo_y="Retención"),
    texto("titulo_accion", ["### Contactar a quien cumple 34 días sin comprar: cuánto vale y cuánto cuesta (margen, "
                            "pesos de dic-2025)"], pos(0, 13, 6, 1)),
    contador("vale", "vale_cuesta", "vale_texto", "Vale: margen anual por cada 10 puntos de retención",
             pos(0, 14, 2, 2)),
    contador("cuesta", "vale_cuesta", "cuesta_texto", "Cuesta: contacto y cupón en un año", pos(2, 14, 2, 2)),
    contador("equilibrio", "vale_cuesta", "equilibrio_texto", "Se paga si recupera", pos(4, 14, 2, 2), tam=20),
    filtro("filtro_costo", "Costo por contacto ($)", "opciones_costo", "costo_contacto", "costo_contacto",
           ["vale_cuesta"], "100", pos(0, 16, 2, 1)),
    filtro("filtro_cupon", "Cupón para quien vuelve (%)", "opciones_cupon", "cupon", "cupon", ["vale_cuesta"], "10",
           pos(2, 16, 2, 1)),
    contador("supuestos", "vale_cuesta", "supuestos_texto", "Supuestos", pos(4, 16, 2, 1), tam=11),
    tabla("tabla_locales", "tiendas",
          [("tienda", "Local principal"), ("activos_jun", "Activos al 30-jun"), ("retenidos", "Retenidos"),
           ("tasa", "Retención"), ("intervalo_95", "Intervalo de confianza 95%"), ("lectura", "Lectura")],
          "Detalle por local (sin ranking: con 33 a 46 clientes por local, solo se separa el que tiene su intervalo "
          "completo bajo la tasa de la cadena)", pos(0, 17, 6, 5)),
    contador("pie", "estado", "pie", "Fuente y estado de los datos", pos(0, 22, 6, 1), tam=11),
]

pagina_definicion = [
    texto("titulo_def", ["## Decisión de definición: la ventana de cliente activo",
                         "La definición mueve el número, pero no la conclusión: con 30, 90 y 180 días más de la mitad "
                         "de los activos de diciembre son reposición."], pos(0, 0, 6, 2)),
    barras("grafico_ventanas", "ventanas", "ventana_dias", "valor", "medida",
           "Retención y reposición según la ventana (% de clientes)", pos(0, 2, 4, 6),
           mapeo={"Retención": NAVY, "Reposición": NARANJO}, agrupadas=True, formato_y=PCT,
           titulo_x="Ventana de cliente activo (días)", titulo_y="% de clientes"),
    texto("por_que_90", ["### Por qué 90 días",
                         "La mitad de los clientes vuelve en 13 días y el 93% de las pausas entre compras dura menos "
                         "de 90 días. Con 30 días, más de un cuarto de las pausas normales se leería como pérdida.",
                         "",
                         "**Regla de lectura** (fijada antes de calcular): la cadena repone si más del 50% de los "
                         "activos al 31-dic no estaba activo al 30-jun."], pos(4, 2, 2, 6)),
    tabla("tabla_ventanas", "ventanas_tabla",
          [("ventana", "Ventana"), ("retencion", "Retención jun → dic"), ("reposicion", "Reposición en dic"),
           ("lectura", "Lectura")], "Detalle por ventana", pos(0, 8, 6, 3)),
    barras("grafico_antiguedad", "antiguedad", "tipo_cliente", "tasa", "grupo",
           "Retención con 90 días por tipo de cliente (% de clientes)", pos(0, 11, 4, 5),
           mapeo={"Todos los clientes": NAVY, "Sin clientes con alta posterior a su primera compra": NARANJO},
           agrupadas=True, formato_y=PCT, titulo_x="Tipo de cliente", titulo_y="Retención"),
    texto("nota_antiguedad", ["### Sensibilidad de la antigüedad",
                              "El tipo de cliente se define por la fecha de alta en la tarjeta. 311 clientes tienen "
                              "el alta después de su primera compra; todos tienen el alta en 2025, así que la base "
                              "heredada no cambia. Sin ellos, la brecha entre nuevos y heredada se reduce de 8,9 a "
                              "3,7 puntos: no es concluyente. La conclusión (repone) no depende de esta definición."],
          pos(4, 11, 2, 5)),
]

tablero = {
    "datasets": [dataset(n) for n in DATASETS],
    "pages": [
        {"name": "principal", "displayName": "Retención y reposición", "pageType": "PAGE_TYPE_CANVAS",
         "layout": pagina_principal},
        {"name": "definicion", "displayName": "Decisión de definición", "pageType": "PAGE_TYPE_CANVAS",
         "layout": pagina_definicion},
    ],
    "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}},
}

with open(SALIDA, "w", encoding="utf-8") as f:
    json.dump(tablero, f, ensure_ascii=False, indent=2)
print("Tablero generado:", SALIDA)
