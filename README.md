# Multitienda del Sur SpA · Entrega 1

**IIC 3752 Tecnologías para la Inteligencia de Negocios · 2026-2 · Grupo 3**

**NOTA: Se recomienda fuertemente correr este código desde Google Colab, entorno en el que fue desarrollado y probado para minimizar posibles problemas de compatibilidad y dependencias al ejecutarlo localmente**

Cadena de datos de la Entrega 1\. Responde la **Pregunta 1** del caso retail: *¿la cadena está reteniendo a sus clientes o los está reponiendo con compradores de una sola vez?*

Un solo notebook lee las cuatro fuentes del caso, las limpia, construye el modelo dimensional, calcula la métrica y genera los gráficos y tablas del informe. Todas las cifras del informe salen de esta ejecución.

---

## Contenido del repositorio

```
.
├── pipeline_multitienda.ipynb        # la cadena completa (Etapas 1 a 5)
├── requirements.txt                  # dependencias de Python
├── README.md
└── data/
    ├── origen_pos.sqlite             # fuente 1: base del POS (SQLite)
    ├── origen_maestro_productos.csv  # fuente 2: maestro de productos (CSV ; cp1252)
    ├── origen_promociones.json       # fuente 3: campañas (JSON)
    └── ipc_2025_respaldo.json        # copia de la fuente 4 (API mindicador.cl), solo si la API no responde
└── salidas/                          # resultados de la última ejecución (los cita el informe)
    ├── *.csv                         # métrica, cortes, cobertura, caso de negocio, bitácora
    ├── modelo_dimensional.png
    └── graficos/G1 … G5.png
```

&nbsp;

`salidas/` se versiona como evidencia de las cifras del informe; al correr el notebook se regenera con el mismo contenido. Las carpetas intermedias `raw/`, `plata/` y `oro/` también las crea el notebook, pero no se suben (están en `.gitignore`).

La cuarta fuente, el IPC mensual 2025, se descarga en vivo desde `https://mindicador.cl/api/ipc/2025`. `ipc_2025_respaldo.json` es una copia de esa misma respuesta y solo se usa si la API no contesta después de tres intentos. Si eso ocurre, la bitácora de carga lo deja registrado.

---

## Opción A: Google Colab (recomendada)

Colab ya trae Java, que PySpark necesita. No hay que instalar nada a mano.

1. Abrir [Google Colab](https://colab.research.google.com) → **Archivo → Subir notebook** → elegir `pipeline_multitienda.ipynb`.  
2. **Entorno de ejecución → Ejecutar todas.**  
3. Cuando la celda **"Archivos de entrada"** muestre el botón *Elegir archivos*, **subir los tres archivos** de `data/`: `origen_pos.sqlite`, `origen_maestro_productos.csv` y `origen_promociones.json`. Se pueden seleccionar los tres juntos. El notebook los mueve solo a `data/`.  
   - Opcional: subir también `ipc_2025_respaldo.json` desde el panel de archivos a la carpeta `data/`, para tener el respaldo si la API falla.  
4. Esperar a que termine. No hay que tocar nada más.

**Tiempo:** unos **2 a 3 minutos** en total (1 minuto instalando PySpark, más la ejecución).

## Opción B: entorno local

Requisitos: **Python 3.10 a 3.12** y **Java 17**, que PySpark 3.5 necesita. Verificar con `java -version`.

```sh
git clone <URL_DEL_REPOSITORIO>
cd <carpeta_del_repositorio>
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt jupyter
jupyter nbconvert --to notebook --execute pipeline_multitienda.ipynb --output pipeline_ejecutado.ipynb
```

&nbsp;

Hay que ejecutarlo desde la carpeta del repositorio, para que encuentre `data/`. También se puede abrir con `jupyter notebook` y ejecutar todas las celdas.

**Tiempo:** alrededor de **1 minuto**.

---

## Qué hace cada etapa

| Etapa | Qué hace | Herramienta | Deja |
| :---- | :---- | :---- | :---- |
| 1\. Extracción | Lee las 4 fuentes sin modificarlas y verifica que no se pierdan filas | PySpark | `raw/{tabla}/{fecha_carga}/` en Parquet |
| 2\. Limpieza | Fechas, montos, descuentos, duplicados, RUT, categorías, llaves huérfanas; cada regla queda en la bitácora | pandas | `plata/*.parquet` |
| 3\. Modelo dimensional | Estrella con `fact_venta_linea` y 5 dimensiones; 15 controles de integridad que detienen la ejecución si fallan | pandas | `oro/*.parquet` |
| 4\. Métrica | Retención semestral con ventana de 90, 180 y 30 días; cortes por tipo de cliente, tienda, canal y segmento | pandas | `salidas/*.csv` |
| 5\. Evidencia | 5 gráficos, caso de negocio en pesos y resumen de cifras | matplotlib | `salidas/graficos/*.png` |

&nbsp;

---

## Qué se espera ver al final

La penúltima celda imprime el resumen con las cifras del informe:

```
RESPUESTA A LA P1
La cadena está REPONIENDO clientes más que reteniéndolos.
- Con ventana de 90 días, de 487 clientes activos al 30 de junio siguen activos 202 al 31 de diciembre:
  retención de 41.5%. Se perdieron 285.
- De los 661 activos en diciembre, 459 (69.4%) no estaban activos en junio
  (415 nuevos y 44 reactivados).
- Con 180 días: retención 58.6% y reposición 58.7%. La conclusión se mantiene;
  la definición mueve la retención en +17.2 puntos.
...
```

&nbsp;

Otros números de control que deben coincidir:

| Control | Valor esperado |
| :---- | :---- |
| Boletas en origen | 6.338 |
| Boletas válidas 2025 | 6.239 |
| Visitas identificadas usadas en la retención | 5.385 (85,0% del origen) |
| Líneas en `fact_venta_linea` | 14.532 |
| Venta total 2025 | \$79.964.082 |
| Controles de integridad del modelo | todos `OK` |

&nbsp;

Archivos generados en `salidas/`:

| Archivo | Uso en el informe |
| :---- | :---- |
| `metrica_retencion.csv` | Ficha de la métrica (D): resultado con 30, 90 y 180 días |
| `retencion_por_tipo_cliente.csv`, `retencion_por_tienda_principal.csv`, `retencion_por_mezcla_canal.csv`, `retencion_por_segmento.csv` | Cortes de la métrica |
| `recompra_90_nuevos_por_mes.csv`, `clientes_y_venta_mensual_por_tipo.csv`, `clientes_con_compra_por_tienda_y_mes.csv` | Evidencia complementaria (E) |
| `caso_de_negocio.csv` | Hallazgo traducido a pesos de diciembre de 2025 (E) |
| `cobertura.csv` | Cobertura de datos (E) |
| `bitacora_supuestos.csv` | Anexo: bitácora de supuestos (34 reglas) |
| `modelo_dimensional.png` | Diagrama del modelo (C) |
| `graficos/G1` a `G5` | Visualizaciones (E) |

&nbsp;

---

## Problemas frecuentes

| Síntoma | Causa y solución |
| :---- | :---- |
| `Faltan archivos en data/` | No se subieron los tres archivos de origen. Volver a ejecutar la celda de archivos y subirlos. |
| `Intento 1 falló: ... Read timed out` en la celda del IPC | Normal: mindicador.cl a veces demora. El notebook reintenta; si los tres intentos fallan, usa `data/ipc_2025_respaldo.json`. |
| `La API del IPC no respondió y no existe data/ipc_2025_respaldo.json` | Sin internet y sin respaldo. Copiar `ipc_2025_respaldo.json` a `data/`. |
| `JAVA_HOME is not set` o un error de Java al iniciar Spark (solo en local) | Instalar Java 17 o usar Colab. |
| Un `AssertionError` con texto como `Falla el control` | Uno de los controles de integridad detectó datos distintos a los esperados. Revisar que los archivos de `data/` sean los originales del caso. |

&nbsp;

---

## Notas

- El notebook es **determinístico**: con los mismos archivos de entrada produce siempre las mismas cifras. La única parte que viene de internet, el IPC, es la serie cerrada de 2025\.  
- La carpeta de carga en `raw/` se nombra con la fecha de ejecución (`raw/{tabla}/AAAAMMDD/`). Eso no cambia ninguna cifra.  
- Decisiones de limpieza, modelo y métrica: ver `salidas/bitacora_supuestos.csv` y las celdas de texto del notebook.  
- Uso de IA: el código fue desarrollado con apoyo de Claude (Anthropic). El detalle está en la declaración del informe.
