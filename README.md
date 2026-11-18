# Multitienda del Sur SpA · Cadena de datos de retención de clientes

IIC 3752 Tecnologías para la Inteligencia de Negocios · 2026-2 · Grupo 3

Pregunta de negocio: *¿la cadena está reteniendo a sus clientes o los está reponiendo con compradores de una sola vez?*

La cadena integra cuatro fuentes (POS en SQLite, maestro de productos en CSV, campañas en JSON e IPC desde la API de mindicador.cl), las procesa en capas bronce, plata, oro y semántica, ejecuta pruebas de calidad en cada capa y publica las tablas que consumen el informe y el tablero. El mismo código se ejecuta en Databricks (tablas Delta en Unity Catalog) y en local (Parquet).

## Estructura

```
.
├── cadena/
│   ├── comun.py                  # configuración, lectura/escritura, pruebas, bitácora y definición de la métrica
│   ├── 00_preparar_entorno.py    # esquemas y Volumes
│   ├── 01_bronce.py              # carga de las fuentes y pruebas por fuente
│   ├── 02_plata.py               # limpieza, tipos y cobertura
│   ├── 03_oro.py                 # modelo dimensional y diccionario de datos
│   ├── 04_semantica.py           # métrica, metric view, tablas de consumo y estado de la corrida
│   └── responsables_bitacora.csv # responsable y fecha de cada regla de la bitácora
├── data/                         # fuentes y respaldo del IPC
├── salidas/                      # cifras de la última corrida en CSV
├── tablero/
│   ├── retencion_clientes.lvdash.json  # tablero AI/BI de Databricks
│   ├── generar_tablero.py        # genera el tablero y define sus consultas
│   └── app_local.py              # copia local del tablero (Streamlit), con las mismas consultas
├── databricks.yml                # definición del Job
├── correr_local.py               # ejecución local
└── requirements.txt
```

## Capas

| Capa | Contenido | Tablas principales |
|---|---|---|
| bronce | Fuentes sin transformar, con origen y corrida | `bronce.pos_*`, `bronce.productos`, `bronce.promo_*`, `bronce.ipc_mensual` |
| plata | Datos tipados y limpios según las reglas de la bitácora | `plata.boletas`, `plata.detalle`, `plata.clientes`, `plata.productos` |
| oro | Esquema en estrella: `fact_venta_linea` y dimensiones de fecha, cliente, tienda, producto y campaña | `oro.fact_venta_linea`, `oro.dim_*`, `oro.diccionario_datos` |
| semántica | Estado de cada cliente por ventana, metric view y tablas de consumo | `oro.cliente_ventana`, `oro.mv_retencion`, `oro.metrica_retencion`, `oro.retencion_por_*`, `oro.caso_negocio`, `oro.contacto_campanas` |
| control | Resultados de pruebas, bitácoras, cobertura, linaje y estado de cada corrida | `control.pruebas_resultados`, `control.bitacora_carga`, `control.bitacora_supuestos`, `control.cobertura`, `control.linaje_metrica`, `control.estado_corrida` |

- **Carga completa:** cada corrida reconstruye las tablas de datos. Dos corridas con las mismas fuentes producen las mismas claves y cifras. Las tablas de `control` acumulan una entrada por corrida.
- **Pruebas:** cada capa valida sus datos antes de publicarlos. Una prueba bloqueante que falla detiene el Job sin modificar la capa, por lo que el tablero conserva la última corrida válida. Las pruebas de advertencia permiten continuar y quedan indicadas en el pie del tablero.
- **Falla de la API:** cuando la API responde, la respuesta se guarda como respaldo con su fecha de descarga (`ipc_2025_respaldo.json` en el Volume `bronce.fuentes`). Tras tres intentos fallidos se usa ese respaldo; el origen y la fecha quedan en `control.bitacora_carga` y en el pie del tablero. Sin respaldo, la corrida se detiene.
- **Campaña de contacto:** `oro.contacto_campanas` simula, con las visitas de 2025, campañas semanales, quincenales y mensuales que contactan a quien cumple 34 días sin comprar, y calcula su costo con las cifras informadas por Marketing (mensaje de $100, 4 horas de preparación por campaña a $15.000 la hora, cupón de 10% con tope de $5.000 y canje de 15%). Los supuestos están en `comun.py` y en las reglas K07 y K08 de la bitácora.
- **Dueños:** Marketing es dueño del tablero; el área del ERP responde por la extracción de los datos; la Gerencia Comercial es dueña de la definición de cliente activo.
- **Linaje:** `control.linaje_metrica` sigue cada campo que entra en la métrica desde su origen, pasando por la regla de limpieza y el modelo, hasta la pantalla que lo muestra.

## Ejecución en Databricks

1. Workspace → *Create* → *Git folder*, con la URL de este repositorio.
2. Jobs & Pipelines → *Create job*, cómputo *Serverless*:
   - Parámetros: `catalogo` = `workspace`, `url_ipc` = `https://mindicador.cl/api/ipc/2025`, `id_corrida` = `{{job.run_id}}`.
   - Cinco tareas de tipo *Notebook*, cada una dependiente de la anterior: `cadena/00_preparar_entorno.py`, `01_bronce.py`, `02_plata.py`, `03_oro.py`, `04_semantica.py`.
   - Alternativa con la CLI de Databricks: `databricks bundle deploy && databricks bundle run cadena_retencion`.
3. *Run now*. Las tablas quedan en el catálogo indicado y los CSV en el Volume `oro.salidas`.

Si el catálogo principal del workspace tiene otro nombre, se debe ajustar el parámetro `catalogo`.

## Tablero

El tablero lee la metric view `oro.mv_retencion` y las tablas de `oro` y `control`, de modo que muestra siempre la última corrida de la cadena.

- **Retención y reposición:** pregunta, audiencia y frecuencia; filtros de ventana (30, 90 o 180 días) y tipo de cliente; retención y reposición con su comparación, regla de lectura y cobertura; base activa de junio y diciembre; retención por local con intervalo de confianza; valor y costo de la campaña de contacto, con frecuencia, costo por mensaje y tasa de canje ajustables, y clientes reactivados que la pagan; detalle por local; pie con fuente, dueños, fecha de actualización, resultado de las pruebas y origen del IPC.
- **Decisiones:** las tres ventanas lado a lado, la sensibilidad de la retención por antigüedad y el costo de la campaña según su frecuencia (semanal, quincenal o mensual).
- **Calidad de datos:** pruebas de la última corrida, historial de corridas, origen del IPC y linaje de la métrica.

Importación:

1. *Dashboards* → *Create dashboard* (flecha) → *Import dashboard from file* → `tablero/retencion_clientes.lvdash.json`.
2. Seleccionar el SQL warehouse del workspace y publicar con *Publish*.
3. Para que el tablero se refresque al terminar la cadena, agregar al Job una sexta tarea de tipo *Dashboard*, dependiente de `semantica`, que apunte a este tablero.

Si el catálogo no es `workspace`: `python tablero/generar_tablero.py <catalogo>` y volver a importar.

## Ejecución local

Requisitos: Python 3.10 a 3.12 y Java 17 o superior.

```bash
pip install -r requirements.txt
python correr_local.py
```

Tiempo aproximado: 45 segundos. Las tablas quedan en `lakehouse_local/` y los CSV en `salidas/`.

Copia local del tablero, con las mismas consultas y cifras que la versión de Databricks:

```bash
streamlit run tablero/app_local.py
```

## Resultado esperado

Al final de la corrida se imprime `Corrida <id> publicada: 70/70 pruebas en verde, IPC desde api`. Si la API no responde, el resultado es `69/70` con `IPC desde respaldo`.

| Control | Valor esperado |
|---|---|
| Boletas en origen | 6.338 |
| Visitas identificadas usadas en la retención | 5.385 (85,0% del origen) |
| Filas de `fact_venta_linea` | 14.532 |
| Retención con ventana de 90 días | 41,5% (202 de 487) |
| Reposición con ventana de 90 días | 69,4% (459 de 661): la cadena repone |
| Retención con ventanas de 180 y 30 días | 58,6% y 18,6% |
| Margen anual por cada 10 puntos de retención | $2,03 millones (pesos de diciembre de 2025) |
| Campaña mensual de contacto | $1,26 millones al año; cada campaña se paga si reactiva 3 de 148 contactados |

## Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `Falta <archivo> en /Volumes/...` | La carpeta `data/` no está en el Git folder. Subir los cuatro archivos al Volume `bronce.fuentes`. |
| `Intento 1 falló` en la capa bronce | La API no respondió. La cadena continúa con el respaldo y lo advierte. |
| `Fallaron N pruebas bloqueantes en ...` | Una fuente cambió. Revisar `control.pruebas_resultados` para la última `id_corrida`. |
| Error de Java en la ejecución local | Instalar Java 17 o superior y verificar con `java -version`. |
