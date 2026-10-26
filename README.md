# Proyecto Final MLOps 17 — Pipeline de predicción en GCP

**Omar Rodrigo Huaman Rivera** · DataPath, MLOps Engineer 17ª Edición

Automatización de un pipeline de predicción de Machine Learning repartido en
**tres proyectos de Google Cloud**, según la arquitectura del enunciado
`Proyecto.pdf` y el diagrama `Proyecto_Final.png`.

---

## 1. Arquitectura

```
PROYECTO FUENTE                 PROYECTO PROCESAMIENTO            PROYECTO PRODUCCIÓN
dp-...-pf-fuente                dp-...-pf-proc                    dp-...-pf-prd

hist_table.input_data   ──┐
hist_table.selected_f.  ──┤     [Vertex AI Pipeline]
                          └──▶  ① BigqueryQueryJobOp
                                   └─ CALL prc_preparar_universo
                                      └─▶ ds_procesamiento.table_output
                                ② prediction (Python)
                                   ├─ lee table_output
                                   ├─ carga el modelo del bucket     ──▶ ds_produccion.
                                   ├─ escribe las predicciones            ba_modelo_propension_inscripcion
                                   └─ archiva el universo            ──▶ ds_produccion.
                                                                         ba_aux_modelo_universo

                                gs://dp-...-pf-modelo   (solo lectura)
                                  linear_regression.joblib
                                gs://dp-...-pf-vertex   (artefactos)
                                  demo/pipeline_prediction.yaml

                                Cloud Run Job  ◀── Cloud Scheduler (pausado)
```

El modelo es un `Lasso` de scikit-learn que predice el **logaritmo del precio de
venta de viviendas** (dataset Ames Housing), con 36 variables ya normalizadas
entre 0 y 1. Es el mismo modelo que recorre el bootcamp desde el Módulo 2.

---

## 2. Las tres cuentas de servicio

Una sola cuenta accede a los datos de los tres proyectos, y cada servicio de
orquestación tiene la suya con el mínimo imprescindible.

| Cuenta | Qué hace | Permisos |
|---|---|---|
| `sa-vertex-pipeline` | Ejecuta el pipeline y toca BigQuery de los 3 proyectos | ver tabla §3 |
| `sa-cloud-run-job` | Lanza el PipelineJob | `aiplatform.user`, `iam.serviceAccountTokenCreator`, `storage.objectViewer`, **`iam.serviceAccountUser` sobre `sa-vertex-pipeline`** |
| `sa-scheduler` | Dispara el Cloud Run Job | `run.invoker` sobre el job, y nada más |
| `sa-github` | CI/CD | `run.admin`, `artifactregistry.writer`, escritura en el bucket, `serviceAccountUser` sobre las dos anteriores |

---

## 3. Por qué cada permiso es necesario

Esta sección responde al punto 5 del enunciado.

### Lectura en el proyecto fuente — `BigQuery Data Viewer`

El componente ① lee `hist_table.input_data` desde el proyecto de procesamiento.
Sin este rol, `validate_data` devuelve `false` y el pipeline entra en la rama de
error. **Se concede solo lectura, deliberadamente**: son tablas que en un
entorno real pertenecen a otro equipo y las puebla un proceso ajeno, por lo que
no corresponde poder editarlas.

### Escritura en el proyecto de producción — `BigQuery Data Editor` + `Data Viewer`

El componente ② inserta las predicciones en `ds_produccion`. Aquí sí hacen falta
los dos roles: la tabla la crea y alimenta nuestro propio proceso, y el editor
es imprescindible para el `INSERT`. El visor acompaña para poder leer el
histórico.

### Lectura del modelo en Cloud Storage — `Storage Object Viewer`

El componente ② descarga `linear_regression.joblib`. **El modelo vive en su
propio bucket** y la cuenta de servicio solo tiene `objectViewer` sobre él, que
es literalmente lo que pide el punto 5 del enunciado.

La separación no es cosmética. Vertex escribe los artefactos de cada ejecución
bajo `pipeline_root`, y eso exige permiso de escritura; si el modelo compartiera
bucket con esos artefactos, cualquier rol que habilite lo segundo permitiría
también **borrar o sobrescribir el modelo**. Con dos buckets el modelo es
inmutable para el pipeline y se puede demostrar en una captura de IAM.

| Bucket | Contenido | `sa-vertex-pipeline` |
|---|---|---|
| `dp-...-pf-modelo` | `linear_regression.joblib` | `objectViewer` |
| `dp-...-pf-vertex` | `pipeline_root` y el YAML compilado | `objectUser` |

El de artefactos usa `objectUser`, no `objectAdmin`: concede lo necesario para
escribir y leer objetos, pero no `storage.objects.setIamPolicy`. Lo peor que
puede ocurrir ahí es perder artefactos desechables de una corrida.

El profesor es explícito al respecto en `Sesión 3 - GCP III [00:51:53]`: asigna
solo `Storage Object Viewer` y aclara que los tres permisos del ejemplo anterior
eran porque **en desarrollo** había que crear un archivo en el bucket, mientras
que el proceso que corre en Cloud Run no crea nada.

### Tablas temporales en procesamiento — `Data Editor` + `Data Viewer`

`table_output` la crea y trunca el propio pipeline en cada ejecución.

---

## 3.1 Las dos tablas de producción

`table_output` se trunca al inicio de cada corrida, así que el detalle de qué se
puntuó no sobrevive. Por eso el componente ② escribe **dos** tablas:

| Tabla | Forma | Para qué |
|---|---|---|
| `ba_modelo_propension_inscripcion` | 4 columnas | La salida oficial. Lo que consume el negocio y los dashboards |
| `ba_aux_modelo_universo` | 36 + 7 columnas, particionada por `process_date` | El archivo: auditoría del score, monitoreo de drift (Módulo 8) y reproceso |

La auxiliar guarda las 36 variables de entrada **junto al resultado en la misma
fila**. El emparejamiento se hace en pandas, donde el orden sí está garantizado;
una tabla de BigQuery no tiene orden, así que si las dos tablas se escribieran
por separado el vínculo fila-a-fila se perdería y no hay llave natural con la
que recuperarlo (comprobado: de 1.314 filas solo 1.313 combinaciones de features
son únicas).

Además lleva tres columnas que la tabla oficial no puede tener:

- `prediction_log` — la salida cruda del modelo, sin truncar
- `sale_price_usd` — `exp(prediction_log)`, el precio reconstruido
- `input_overall_qual` — el parámetro con el que se definió el universo

El modelo es un `Lasso` entrenado sobre **`ln(SalePrice)`**: el `ytrain.csv` del
curso trae `12.211060187041213`, no `201443`. El código de clase hace
`astype('int64')` sobre esa salida, lo que deja la tabla oficial con enteros
entre 10 y 13. Se respeta tal cual, y el precio en dólares se conserva en la
auxiliar a partir del valor sin truncar.

### Suplantación de cuentas — `iam.serviceAccountUser`

Quien ejecuta el Cloud Run Job es `sa-cloud-run-job`, pero el pipeline debe
correr como `sa-vertex-pipeline`, que es la que tiene los accesos a datos. Para
que una cuenta pueda lanzar algo *en nombre de* otra, necesita este rol sobre
ella. Es el permiso que más se olvida y sin él el `job.submit()` falla.

### Herencia

Los permisos de datos se conceden a nivel de **dataset**, no de tabla, y se
heredan a todas las tablas que contiene. En la consola, un permiso heredado
aparece con un icono propio y no se puede editar desde la tabla.

### Roles a nivel de proyecto, y uno que sobra

Los roles de proyecto de `sa-vertex-pipeline` en procesamiento son los del
script de permisos de clase (`sa_process.txt`, Módulo 6 Sesión 1):
`bigquery.jobUser` para lanzar consultas, `bigquery.readSessionUser` para que
`to_dataframe()` pueda usar la Storage Read API, `artifactregistry.reader`,
`aiplatform.user`, `bigquery.resourceViewer` y los de logging y monitoring.

De esa lista el pipeline no usa `resourceViewer`. Ninguno de sus permisos lee
datos —no incluye `tables.get` ni `tables.getData`— y el único con alcance real
es `bigquery.jobs.listAll`, que permite ver el texto SQL y los metadatos de los
jobs que ejecute cualquier otro usuario del proyecto. En un entorno real se
retiraría; se conserva para mantener la misma asignación de permisos aplicada
en clase.

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

## 4.1 Despliegue continuo

Cada `push` a `main` dispara `.github/workflows/cd_vertex.yaml`, que hace cuatro
cosas en orden:

1. **Compila el pipeline.** Ejecuta `my_app/pipeline_prediction.py`, que genera
   el YAML. Si el código del pipeline está roto, el build falla aquí y no se
   despliega nada: es la parte de *integración continua*.
2. **Sube el YAML compilado** a `gs://dp-mlops17-ohuaman-pf-vertex/demo/`.
3. **Construye y publica la imagen** del Cloud Run Job en Artifact Registry.
4. **Redespliega el Cloud Run Job** con esa imagen y con todas las variables de
   entorno, incluidas las tres tablas y la cuenta de servicio con la que debe
   correr el pipeline.

El workflow se autentica como `sa-github`, que no toca datos: solo despliega.

### Secretos que necesita el repositorio

| Secreto | Qué es |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Clave JSON de `sa-github`. Es lo único sensible del repositorio, y por eso `.gitignore` bloquea cualquier `*.json` |
| `GOOGLE_PROJECT` | `dp-mlops17-ohuaman-pf-proc`, el proyecto de procesamiento |
| `NOTIFICATION_EMAIL` | Destinatario de la notificación del `ExitHandler`. Es configuración, no código: así no queda un correo personal en el repositorio |

Los otros dos proyectos no son secretos y van escritos en el propio workflow,
porque sus identificadores no dan acceso a nada por sí solos.

---

## 5. Nota de migración: el stack del curso quedó obsoleto

El material de clase fija `kfp==1.8.21`. Al lanzar el pipeline, Vertex AI lo
rechaza:

```
SchemaVersion < 2.0.0 are not longer supported.
```

Google retiró el soporte de ese formato, de modo que la migración a **KFP 2** es
obligatoria. Los cambios:

| Clase (kfp 1.8) | Aquí (kfp 2.17) |
|---|---|
| `kfp.v2.dsl`, `@component` | `kfp.dsl`, `@dsl.component` |
| `dsl.Condition` | `dsl.If` |
| `string.Template` sobre un parámetro | componente `build_query` |
| compila a `.json` | compila a `.yaml` |

El tercero es el único cambio de diseño: KFP 2 no permite interpolar parámetros
del pipeline dentro de una cadena, así que la llamada parametrizada al stored
procedure se construye dentro de un componente. La estructura —validación,
condicional de dos ramas, `ExitHandler` con notificación— es la de clase.

---

## 6. Costo

Calculado con el método del PPT `Costos_Pipeline_Prediccion.pptx` (Módulo 8).

| Servicio | Situación | Costo |
|---|---|---|
| BigQuery consultas | 1 TB/mes gratis; aquí se leen megabytes | $0 |
| BigQuery almacenamiento | 10 GB/mes gratis | $0 |
| Cloud Storage | capa gratuita | ~$0 |
| Cloud Scheduler | 3 jobs gratis | $0 |
| Cloud Run Job | ejecuciones de segundos | ~$0 |
| **Vertex AI Pipelines** | e2-standard-4 a $0.160814/h | **~$0.02 por ejecución** |

Medidas de control aplicadas: presupuesto con alertas al 50/90/100 % sobre toda
la cuenta de facturación, Scheduler creado pero **en pausa**, e imagen basada en
`python:3.11-slim` en lugar de `python:3.8` para no agotar la capa gratuita de
Artifact Registry.
