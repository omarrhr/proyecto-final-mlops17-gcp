# Proyecto Final MLOps 17 — Pipeline de predicción en GCP

Omar Rodrigo Huaman Rivera · DataPath, MLOps Engineer 17ª Edición

Pipeline de predicción desplegado sobre tres proyectos de Google Cloud, según la
arquitectura del enunciado (`Proyecto.pdf` y `Proyecto_Final.png`).

El modelo es un Lasso de scikit-learn que predice el logaritmo del precio de
venta de viviendas (Ames Housing), con 36 variables de entrada.

---

## 1. Arquitectura

```
PROYECTO FUENTE              PROYECTO PROCESAMIENTO          PROYECTO PRODUCCIÓN
dp-...-pf-fuente             dp-...-pf-proc                  dp-...-pf-prd

hist_table.input_data  ──┐
hist_table.selected_f. ──┤   Vertex AI Pipeline
                         └─▶ ① BigqueryQueryJobOp
                                CALL prc_preparar_universo
                                └─▶ ds_procesamiento.table_output
                             ② prediction (Python)
                                lee table_output
                                carga el modelo del bucket   ──▶ ba_modelo_propension_inscripcion
                                escribe las predicciones     ──▶ ba_aux_modelo_universo

                             gs://dp-...-pf-modelo    linear_regression.joblib
                             gs://dp-...-pf-vertex    artefactos de Vertex

                             Cloud Run Job  ◀── Cloud Scheduler (0 6 * * *)
```

**Fuente**: contiene los datos de entrada. El pipeline solo los lee.

**Procesamiento**: ejecuta todo el cómputo. Aquí están el pipeline de Vertex, el
bucket con el modelo, el stored procedure, la tabla temporal, el Cloud Run Job y
el Cloud Scheduler.

**Producción**: recibe el resultado. Contiene las dos tablas finales.

---

## 2. Pipeline

**Componente 1 — Extracción y procesamiento.** Un `BigqueryQueryJobOp` llama al
stored procedure `prc_preparar_universo`, que lee las tablas de la fuente,
aplica las transformaciones y deja `ds_procesamiento.table_output` con las 36
variables que usa el modelo.

**Componente 2 — Predicción.** Python: lee `table_output`, descarga
`linear_regression.joblib` del bucket, ejecuta la predicción y escribe los
resultados en producción.

Antes de los dos hay un paso de validación que comprueba que las tablas de la
fuente existan y sean accesibles. Si falla, el pipeline entra en la rama de
error y no continúa. Todo va dentro de un `ExitHandler`, de modo que termine
bien o mal se envía una notificación por correo.

### Las dos tablas de producción

| Tabla | Columnas | Para qué |
|---|---|---|
| `ba_modelo_propension_inscripcion` | 4 | La salida oficial que pide el enunciado |
| `ba_aux_modelo_universo` | 43 | Las 36 variables de entrada junto al resultado |

La auxiliar existe porque `table_output` se trunca en cada corrida. Sin ella no
quedaría registro de qué datos produjeron cada predicción. Además guarda
`prediction_log`, la salida del modelo sin truncar, y `sale_price_usd`, el
precio reconstruido en dólares.

Las dos se distinguen por `load_date`, de modo que cada ejecución añade su
bloque de 1.314 filas sin pisar las anteriores.

---

## 3. Permisos

Punto 5 del enunciado.

Todo el acceso a datos lo tiene una sola cuenta, `sa-vertex-pipeline`, y se
concede **a nivel de dataset**, no de proyecto.

| Dónde | Rol | Por qué |
|---|---|---|
| `hist_table` (fuente) | `BigQuery Data Viewer` | El componente 1 lee las tablas de entrada. Solo lectura: son tablas de otro equipo y no corresponde poder editarlas |
| `ds_procesamiento` | `Data Editor` + `Data Viewer` | El pipeline crea y trunca `table_output` en cada corrida |
| `ds_produccion` | `Data Editor` + `Data Viewer` | El componente 2 inserta las predicciones. Sin Editor podría leer la tabla pero fallaría al escribir |
| `gs://dp-...-pf-modelo` | `Storage Object Viewer` | El componente 2 descarga el modelo. No lo modifica, así que no necesita más |
| `gs://dp-...-pf-vertex` | `Storage Object User` | Vertex escribe aquí los artefactos de cada ejecución |

**El modelo está en un bucket aparte a propósito.** Vertex necesita permiso de
escritura sobre el bucket donde deja sus artefactos. Si el modelo compartiera
bucket con ellos, ese mismo permiso alcanzaría al modelo y la cuenta podría
sobrescribirlo o borrarlo. Separándolos, el modelo es de solo lectura para el
pipeline.

A nivel de proyecto, `sa-vertex-pipeline` lleva los roles del script de
permisos de clase: `bigquery.jobUser` para lanzar consultas,
`bigquery.readSessionUser` para la Storage Read API, `aiplatform.user`,
`artifactregistry.reader` y los de logging y monitoring. Ninguno da acceso a
datos.

### Las otras cuentas de servicio

| Cuenta | Qué hace | Permisos |
|---|---|---|
| `sa-cloud-run-job` | Lanza el PipelineJob | `aiplatform.user`, `storage.objectViewer` y `iam.serviceAccountUser` sobre `sa-vertex-pipeline` |
| `sa-scheduler` | Dispara el Cloud Run Job | `run.invoker` sobre el job |
| `sa-github` | Despliegue continuo | `run.admin`, `artifactregistry.writer`, escritura en el bucket |

Ninguna de las tres toca datos.

**`iam.serviceAccountUser` es el permiso que más se olvida.** Quien ejecuta el
Cloud Run Job es `sa-cloud-run-job`, pero el pipeline tiene que correr como
`sa-vertex-pipeline`, que es la que tiene los accesos. Para que una cuenta pueda
lanzar algo en nombre de otra necesita este rol sobre ella. Sin él, el
`job.submit()` falla.

---

## 4. Estructura del repositorio

```
my_app/
  pipeline_prediction.py       definición del pipeline (KFP 2)
  pipeline_prediction.yaml     compilado, se sube al bucket
  requirements.txt
despliegue_cloud_run/
  main.py                      lanza el PipelineJob leyendo variables de entorno
  Dockerfile
  requirements.txt
sql/
  componente1.sql              tabla temporal y stored procedure
  componente2_aux.sql          tabla auxiliar del universo puntuado
permisos/
  01_sa_vertex.sh
  02_sa_cloud_run_y_scheduler.sh
.github/workflows/
  cd_vertex.yaml               compila, sube el pipeline, construye y despliega
```

### Automatización

El **Cloud Run Job** no ejecuta la predicción: lanza el pipeline de Vertex y
termina en segundos. Toda su configuración entra por variables de entorno —las
tres tablas, la ruta del modelo y la cuenta de servicio—, así que la misma
imagen sirve para cualquier entorno.

El **Cloud Scheduler** dispara el job todos los días a las 6:00 (`0 6 * * *`,
hora de Lima). Se deja pausado fuera de las demostraciones para no incurrir en
costo.

Cada `push` a `main` dispara el workflow de GitHub Actions, que compila el
pipeline, sube el YAML al bucket, construye la imagen y redespliega el Cloud Run
Job. Necesita tres secretos: `GOOGLE_APPLICATION_CREDENTIALS` (clave de
`sa-github`), `GOOGLE_PROJECT` y `NOTIFICATION_EMAIL`.

---

## 5. Nota sobre la versión de KFP

El material de clase usa `kfp==1.8.21`, pero Vertex AI ya rechaza ese formato:

```
SchemaVersion < 2.0.0 are not longer supported.
```

El pipeline está migrado a KFP 2: `kfp.dsl` en lugar de `kfp.v2.dsl`, `dsl.If`
en lugar de `dsl.Condition`, y compila a `.yaml` en vez de `.json`. KFP 2
tampoco permite interpolar parámetros dentro de una cadena, así que la llamada
al stored procedure se construye en un componente `build_query`. La estructura
—validación, condicional, `ExitHandler`— es la de clase.

---

## 6. Costo

| Servicio | Costo |
|---|---|
| BigQuery (consultas y almacenamiento) | $0, dentro de la capa gratuita |
| Cloud Storage | ~$0 |
| Cloud Scheduler | $0, los 3 primeros jobs son gratis |
| Cloud Run Job | ~$0, ejecuciones de segundos |
| Vertex AI Pipelines | ~$0.02 por ejecución |

Controles aplicados: presupuesto con alertas al 50/90/100 % y el Scheduler en
pausa fuera de las demostraciones.
