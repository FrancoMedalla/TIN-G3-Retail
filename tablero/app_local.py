"""Copia local del tablero: mismas consultas que el dashboard de Databricks, sobre lakehouse_local/.

    python correr_local.py
    streamlit run tablero/app_local.py

MEASURE(medida) se reemplaza por la fórmula de comun.METRICAS, que es la misma definición de la metric view.
"""
import os
import re
import sys

import altair as alt
import streamlit as st

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("RAIZ_REPO", RAIZ)
sys.path[:0] = [os.path.join(RAIZ, "cadena"), os.path.join(RAIZ, "tablero")]

import generar_tablero as gt

NARANJO, NARANJO_SUAVE, GRIS_OSCURO, GRIS, GRIS_CLARO = (gt.NARANJO, gt.NARANJO_SUAVE, gt.GRIS_OSCURO, gt.GRIS,
                                                         gt.GRIS_CLARO)
TABLAS = [("oro", "cliente_ventana"), ("oro", "contacto_campanas"), ("control", "cobertura"),
          ("control", "estado_corrida"), ("control", "pruebas_resultados"), ("control", "linaje_metrica")]

st.set_page_config(page_title="Multitienda del Sur · Retención de clientes", layout="wide")


@st.cache_resource(show_spinner="Iniciando el motor de consultas…")
def motor():
    from comun import METRICAS, leer, spark
    for capa, tabla in TABLAS:
        leer(capa, tabla).createOrReplaceTempView(f"{capa}_{tabla}")
    return spark, METRICAS


@st.cache_data(show_spinner=False)
def consultar(nombre, **parametros):
    spark, metricas = motor()
    sql = gt.DATASETS[nombre][1]
    sql = sql.replace(gt.MV, "oro_cliente_ventana")
    sql = re.sub(rf"{gt.CAT}\.(oro|control)\.(\w+)", r"\1_\2", sql)
    sql = re.sub(r"MEASURE\((\w+)\)", lambda m: f"({metricas[m.group(1)][0]})", sql)
    for clave, valor in parametros.items():
        sql = sql.replace(f":{clave}", f"'{valor}'")
    return spark.sql(sql).toPandas()


def barras(df, cat, val, color, colores, horizontal=False, agrupadas=False, pct=False, alto=320,
           decimales=0):
    """Barras con etiqueta directa; las etiquetas usan coma decimal."""
    df = df.copy()
    df["etiqueta"] = df[val].map(lambda v: (f"{v:.1%}" if pct else f"{v:,.{decimales}f}").replace(",", "X")
                                 .replace(".", ",").replace("X", "."))
    presentes = [c for c in colores if c in set(df[color])]
    escala = alt.Scale(domain=presentes, range=[colores[c] for c in presentes])
    eje_val = alt.Axis(format=".0%" if pct else f",.{decimales}f", title=None, grid=True)
    leyenda = alt.Legend(orient="bottom", title=None, labelLimit=400)
    if horizontal:
        base = alt.Chart(df).encode(y=alt.Y(f"{cat}:N", title=None, sort=None, axis=alt.Axis(labelLimit=200)),
                                    x=alt.X(f"{val}:Q", axis=eje_val))
        texto = base.mark_text(align="left", dx=4, color="#333").encode(text="etiqueta:N")
    elif agrupadas:
        base = alt.Chart(df).encode(x=alt.X(f"{cat}:N", title=None, sort=None, axis=alt.Axis(labelAngle=0)),
                                    xOffset=alt.XOffset(f"{color}:N"), y=alt.Y(f"{val}:Q", axis=eje_val))
        texto = base.mark_text(dy=-7, color="#333").encode(text="etiqueta:N")
    else:
        df["fin"] = df.sort_values("orden").groupby(cat)[val].cumsum()
        df["centro"] = df["fin"] - df[val] / 2
        df["tono"] = df[color].map(lambda c: "#FFFFFF" if colores[c] in (GRIS, NARANJO, GRIS_OSCURO) else "#333333")
        base = alt.Chart(df).encode(x=alt.X(f"{cat}:N", title=None, sort=None, axis=alt.Axis(labelAngle=0)))
        texto = base.mark_text(fontWeight="bold").encode(y=alt.Y("centro:Q"), text="etiqueta:N",
                                                         color=alt.Color("tono:N", scale=None))
        base = base.encode(y=alt.Y(f"{val}:Q", axis=eje_val, stack="zero"), order=alt.Order("orden:Q"))
    barra = base.mark_bar().encode(color=alt.Color(f"{color}:N", scale=escala, legend=leyenda),
                                   tooltip=[alt.Tooltip(f"{cat}:N", title="Categoría"),
                                            alt.Tooltip(f"{color}:N", title="Grupo"),
                                            alt.Tooltip("etiqueta:N", title="Valor")])
    return (barra + texto).properties(height=alto)


def tabla(df, columnas, alto="auto"):
    st.dataframe(df[list(columnas)].rename(columns=columnas), hide_index=True, width="stretch", height=alto)


pag1, pag2, pag3 = st.tabs(["Retención y reposición", "Decisiones", "Calidad de datos"])

with pag1:
    st.markdown("## ¿La cadena está reteniendo a sus clientes o los está reponiendo?")
    st.caption("Multitienda del Sur · clientes identificados del programa · cortes al 30-jun y al 31-dic de 2025. "
               "**Para** Marketing (dueño del tablero), Gerencia Comercial y Finanzas, **cada mes** al cierre: decidir a "
               "quién contactar y qué local revisar.")
    c1, c2, c3 = st.columns(3)
    ventana = c1.selectbox("Ventana de cliente activo (días)", ["30", "90", "180"], index=1)
    tipo = c2.selectbox("Tipo de cliente", ["Todos", "Base heredada", "Nuevo 2025"])
    c3.markdown("**Regla de lectura**, fijada antes de calcular: la cadena *repone* si más del 50% de los activos "
                "al 31-dic no estaba activo al 30-jun.")

    kpi = consultar("kpi", ventana=ventana, tipo=tipo).iloc[0]
    m1, m2, m3 = st.columns(3)
    m1.metric("Tasa de retención semestral", kpi.retencion_texto)
    m1.caption(f"Frente a la meta: {kpi.comparacion_retencion}")
    m2.metric("Tasa de reposición", kpi.reposicion_texto)
    m2.caption(f"Frente al umbral: {kpi.comparacion_reposicion}")
    m3.metric("Respuesta según la regla de lectura", kpi.conclusion_texto)
    m3.caption(f"Cobertura: {kpi.cobertura_texto} (las boletas anónimas no entran)")

    g1, g2 = st.columns(2)
    with g1:
        st.markdown("**Clientes activos al 30-jun y al 31-dic (n.º de clientes)**")
        cas = consultar("cascada", ventana=ventana, tipo=tipo)
        orden = {"Siguen activos (retenidos)": 1, "Perdidos": 2, "Reactivados": 2, "Nuevos": 3}
        cas["orden"] = cas.estado.map(orden)
        colores = {"Siguen activos (retenidos)": GRIS, "Perdidos": GRIS_CLARO, "Reactivados": NARANJO_SUAVE,
                   "Nuevos": NARANJO}
        st.altair_chart(barras(cas, "corte", "clientes", "estado", colores), width="stretch")
        st.caption("En naranjo, los reemplazos: clientes activos en diciembre que no lo estaban en junio.")
    with g2:
        st.markdown("**Tasa de retención semestral por local principal (% de clientes)**")
        tie = consultar("tiendas", ventana=ventana, tipo=tipo)
        colores = {"Bajo la cadena": NARANJO, "No concluyente": GRIS_CLARO, "Sobre la cadena": GRIS_OSCURO}
        st.altair_chart(barras(tie, "tienda", "tasa", "lectura", colores, horizontal=True, pct=True),
                        width="stretch")
        st.caption("En naranjo, el local cuyo intervalo de confianza al 95% queda completo bajo la tasa de la cadena. "
                   "Orden alfabético: sin ranking.")

    st.markdown("### Contactar a quien cumple 34 días sin comprar: cuánto vale y cuánto cuesta")
    st.caption("Margen bruto en pesos de diciembre de 2025. Costos informados por Marketing (17-nov): mensaje de $80 a "
               "$120, 4 horas de preparación por campaña a $15.000 la hora y cupón de 10% (tope $5.000) que solo "
               "cuesta si se canjea.")
    f1, f2, f3 = st.columns(3)
    frecuencia = f1.selectbox("Frecuencia de la campaña", ["Mensual", "Quincenal", "Semanal"])
    costo = f2.selectbox("Costo por mensaje ($)", ["80", "100", "120"], index=1)
    canje = f3.selectbox("Clientes contactados que canjean el cupón (%)", ["10", "12,5", "15"], index=2)
    vc = consultar("vale_cuesta", costo_contacto=costo, canje=canje, frecuencia=frecuencia).iloc[0]
    v1, v2, v3 = st.columns(3)
    v1.metric("Vale: margen anual de 10 puntos más de retención (49 clientes)", vc.vale_texto)
    v2.metric("Cuesta: la campaña durante un año", vc.cuesta_texto)
    v3.metric("Cada campaña se paga si reactiva", vc.equilibrio_clientes)
    v3.caption(vc.equilibrio_detalle)
    d = st.columns(6)
    for col, (titulo, valor) in zip(d, [("Costo por campaña", vc.costo_campana_texto),
                                        ("Contactos por campaña", vc.contactos_texto),
                                        ("Días sin comprar al contacto", vc.dia_texto),
                                        ("Vuelven solos en 90 días", vc.vuelven_texto),
                                        ("Margen anual por cliente reactivado", vc.margen_cliente_texto),
                                        ("Clientes que pagan el año", vc.clientes_anio_texto)]):
        col.caption(titulo)
        col.markdown(f"**{valor}**")

    st.markdown("**Detalle por local**")
    tabla(tie, {"tienda": "Local principal", "activos_jun": "Activos al 30-jun",
                "retenidos": "Siguen activos al 31-dic", "tasa_texto": "Tasa de retención",
                "intervalo_95": "Intervalo de confianza 95%", "tasa_cadena_texto": "Cadena", "lectura": "Lectura"})
    st.caption(consultar("estado").iloc[0].pie)

with pag2:
    st.markdown("## Decisión de definición: la ventana de cliente activo")
    st.caption("La definición mueve el número, pero no la conclusión: con 30, 90 y 180 días, más de la mitad de los "
               "activos de diciembre son reemplazos.")
    d1, d2 = st.columns([2, 1])
    with d1:
        st.markdown("**Tasa de retención semestral y tasa de reposición según la ventana (% de clientes)**")
        ven = consultar("ventanas").sort_values("ventana_dias")
        ven["ventana_dias"] = ven.ventana_dias.astype(str) + " días"
        colores = {"Tasa de retención semestral": GRIS, "Tasa de reposición": NARANJO}
        st.altair_chart(barras(ven, "ventana_dias", "valor", "medida", colores, agrupadas=True, pct=True),
                        width="stretch")
    with d2:
        st.markdown("### Por qué 90 días")
        st.markdown("La mitad de los clientes vuelve en 13 días y el 93% de las pausas entre compras dura menos de 90 "
                    "días. Con 30 días, más de un cuarto de las pausas normales se leería como pérdida.\n\n"
                    "**Regla de lectura**, fijada antes de calcular: la cadena repone si más del 50% de los activos al "
                    "31-dic no estaba activo al 30-jun.")
    tabla(consultar("ventanas_tabla"), {"ventana": "Ventana", "retencion": "Tasa de retención semestral",
                                        "reposicion": "Tasa de reposición", "lectura": "Lectura"})
    a1, a2 = st.columns([2, 1])
    with a1:
        st.markdown("**Tasa de retención semestral con 90 días por tipo de cliente (% de clientes)**")
        colores = {"Todos los clientes": GRIS, "Sin los 311 con alta posterior a su primera compra": NARANJO}
        st.altair_chart(barras(consultar("antiguedad"), "tipo_cliente", "tasa", "grupo", colores, agrupadas=True,
                               pct=True), width="stretch")
    with a2:
        st.markdown("### Sensibilidad de la antigüedad")
        st.markdown("El tipo de cliente se define por la fecha de alta en la tarjeta. 311 clientes tienen el alta "
                    "después de su primera compra; todos tienen el alta en 2025, así que la base heredada no cambia. "
                    "Sin ellos, la brecha entre nuevos y heredada se reduce de 8,9 a 3,7 puntos: no es concluyente. "
                    "La conclusión (repone) no depende de esta definición.")

    st.markdown("## Decisión de frecuencia de la campaña de contacto")
    st.caption("Preparar una campaña cuesta lo mismo sin importar a cuántos clientes llegue, así que la frecuencia "
               "decide el costo. Costos con $100 por mensaje y canje de 15%.")
    e1, e2 = st.columns([2, 1])
    with e1:
        st.markdown("**Costo anual de la campaña según su frecuencia ($ MM, pesos de diciembre de 2025)**")
        fc = consultar("frecuencias_costos")
        fc["orden"] = fc.componente.map({"Preparación (4 h por campaña)": 1, "Cupones canjeados": 2, "Mensajes": 3})
        fc = fc.sort_values(["frecuencia", "orden"])
        colores = {"Preparación (4 h por campaña)": NARANJO, "Cupones canjeados": GRIS, "Mensajes": GRIS_CLARO}
        grafico = barras(fc, "frecuencia", "millones", "componente", colores, decimales=2)
        st.altair_chart(grafico, width="stretch")
        st.caption("En naranjo, la preparación: 4 horas de un analista por campaña, a $15.000 la hora.")
    with e2:
        st.markdown("### Por qué mensual")
        st.markdown("Solo la campaña mensual se paga antes de llegar a la meta: necesita 31 clientes reactivados al "
                    "año (3 por campaña) y la meta son 49. La semanal necesita 91 y la quincenal 53. Además, es el "
                    "ritmo con que Marketing ya trabaja.\n\n"
                    "**Lo que se cede:** el contacto llega entre los días 34 y 63 sin comprar (mediana 45). Si la "
                    "cadena entrega la lista de clientes y la preparación baja, una frecuencia mayor acerca el contacto "
                    "al día 34.")
    tabla(consultar("frecuencias"), {"frecuencia": "Frecuencia", "campanas": "Campañas al año",
                                     "contactos": "Contactos al año", "dias": "Días sin comprar al contacto",
                                     "preparacion": "Preparación", "mensajes": "Mensajes", "cupones": "Cupones",
                                     "total": "Costo anual", "clientes": "Clientes que lo pagan",
                                     "antes_de_la_meta": "¿Antes de la meta (49)?"})

with pag3:
    st.markdown("## Calidad de datos y trazabilidad")
    st.caption("Cada corrida de la cadena ejecuta pruebas en todas las capas. Una prueba que *bloquea* detiene la "
               "publicación; una que *advierte* deja continuar y queda indicada aquí y en el pie.")
    rc = consultar("resumen_calidad").iloc[0]
    q1, q2, q3 = st.columns(3)
    q1.metric("Pruebas aprobadas en la última corrida", rc.aprobadas_texto)
    q2.metric("Advertencias", rc.avisos_texto)
    q3.metric("Origen del IPC", rc.origen_ipc_texto)
    st.markdown("**Pruebas de la última corrida publicada**")
    tabla(consultar("pruebas"), {"capa": "Capa", "id_prueba": "Prueba", "fuente_o_metrica": "Protege",
                                 "que_verifica": "Qué verifica", "severidad": "Tipo", "resultado": "Resultado",
                                 "filas_que_fallan": "Filas que fallan", "accion_si_falla": "Acción si falla"},
          alto=560)
    st.markdown("**Últimas corridas publicadas**")
    tabla(consultar("corridas"), {"id_corrida": "Corrida", "fecha_hora": "Fecha y hora",
                                  "pruebas_aprobadas": "Pruebas aprobadas", "origen_ipc": "Origen del IPC"})
    st.markdown("**Linaje de la métrica: del campo de origen a la pantalla**")
    tabla(consultar("linaje"), {"campo_origen": "Campo de origen", "regla_limpieza": "Regla de limpieza",
                                "campo_modelo": "Campo del modelo", "uso_en_la_metrica": "Uso en la métrica",
                                "pantalla": "Pantalla"})
