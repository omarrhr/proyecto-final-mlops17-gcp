# -*- coding: utf-8 -*-
"""
Pipeline de prediccion en Vertex AI - Proyecto Final MLOps 17
Omar Rodrigo Huaman Rivera

Arquitectura de tres proyectos GCP:
  FUENTE        -> tabla de entrada (solo lectura)
  PROCESAMIENTO -> Vertex, bucket con el modelo, stored procedure y tablas temporales
  PRODUCCION    -> tabla final de predicciones

Componente 1: ejecuta el stored procedure en BigQuery y deja TABLE_OUTPUT.
Componente 2: lee TABLE_OUTPUT, carga el modelo desde Cloud Storage, predice
              y escribe el resultado en el proyecto de produccion.

Basado en el material de clase del Modulo 6:
  - Sesion 2: pipeline_prediction_model_s3_v1-2.ipynb (componente de prediccion,
              validate_data, condicional, ExitHandler y notificacion por correo)
  - Sesion 3: pipeline_bigquery_component.ipynb (BigqueryQueryJobOp)

NOTA DE MIGRACION
  El material de clase usa kfp 1.8.21. Vertex AI ya no admite ese formato
  ("SchemaVersion < 2.0.0 are not longer supported"), asi que el pipeline esta
  portado a kfp 2.x. Los cambios obligados son:
    - @component -> @dsl.component ; kfp.v2.dsl -> kfp.dsl
    - dsl.Condition -> dsl.If
    - la consulta parametrizada ya no se arma con string.Template sobre un
      parametro del pipeline (kfp 2 no interpola parametros en strings);
      se construye dentro de un componente propio, build_query.
    - se compila a YAML en lugar de JSON.
  La estructura y la logica son las mismas de clase.
"""
import os
from typing import List, NamedTuple

from kfp import dsl, compiler

from google_cloud_pipeline_components.v1.vertex_notification_email import VertexNotificationEmailOp
from google_cloud_pipeline_components.v1.bigquery import BigqueryQueryJobOp

# --------------------------------------------------------------------------
# Configuracion
# --------------------------------------------------------------------------
PROYECTO_PROC = 'dp-mlops17-ohuaman-pf-proc'
BUCKET = 'dp-mlops17-ohuaman-pf-vertex'

# El destinatario de la notificacion es configuracion, no codigo: entra como
# parametro del pipeline para no dejar un correo personal en el repositorio.
# El Cloud Run Job lo pasa desde la variable de entorno NOTIFICATION_EMAIL.

PIPELINE_LABEL = {'proyecto': 'final', 'curso': 'mlops17'}
PRC_PREPARAR = os.getenv('PRC_PREPARAR_UNIVERSO',
                         '%s.ds_procesamiento.prc_preparar_universo' % PROYECTO_PROC)


# --------------------------------------------------------------------------
# Componentes
# --------------------------------------------------------------------------
@dsl.component
def error_op(msg: str):
    raise ValueError(msg)


@dsl.component(packages_to_install=['google-cloud-bigquery'])
def validate_data(
    source_input_table: str,
    features_table: str,
) -> NamedTuple('Outputs', [('condition', str)]):
    """Comprueba que las tablas de ingesta del proyecto fuente existan y sean accesibles."""
    from google.cloud import bigquery

    client = bigquery.Client()
    try:
        client.get_table(source_input_table)
        client.get_table(features_table)
        condition = 'true'
    except Exception as e:
        print('No se pudo validar la ingesta:', e)
        condition = 'false'

    from collections import namedtuple
    salida = namedtuple('Outputs', ['condition'])
    return salida(condition)


@dsl.component
def build_query(prc_preparar: str, input_overall_qual: float) -> str:
    """Arma la llamada parametrizada al stored procedure.

    En clase esto se hacia con string.Template fuera del pipeline; kfp 2 no
    permite interpolar parametros del pipeline en un string, asi que se resuelve
    dentro de un componente.
    """
    return (
        'DECLARE input_overall_qual FLOAT64 DEFAULT {valor};\n'
        'CALL `{prc}` (input_overall_qual);'
    ).format(valor=input_overall_qual, prc=prc_preparar)


@dsl.component(
    packages_to_install=[
        'google-cloud-bigquery',
        'google-cloud-bigquery-storage',
        'pandas',
        'numpy',
        'scikit-learn',
        'joblib',
        'db-dtypes',
        'pyarrow',
        'pandas-gbq',
        'google-cloud-storage',
        'pytz',
    ],
)
def prediction(
    project: str,
    source_table: str,
    features_table: str,
    table_id: str,
    table_aux: str,
    path_model: str,
    input_overall_qual: float,
):
    """Lee TABLE_OUTPUT, carga el modelo del bucket, predice y guarda en produccion.

    Escribe dos tablas en el proyecto de produccion:
      - table_id  : la salida oficial, estrecha, la que consume el negocio.
      - table_aux : la foto del universo puntuado, con las 36 variables de
                    entrada y el resultado. Sirve para auditoria, monitoreo de
                    drift (Modulo 8) y reproceso.
    """
    from datetime import datetime
    from io import BytesIO

    import numpy as np
    import pandas as pd
    import pandas_gbq
    from google.cloud import bigquery, storage
    from joblib import load
    from pytz import timezone

    TZ = timezone('America/Lima')
    FORMAT_DATE = '%Y-%m-%d'

    client = bigquery.Client(project=project)

    # Datos ya preparados por el componente 1
    X = client.query('SELECT * FROM `{t}`'.format(t=source_table)).to_dataframe()

    # Features seleccionadas, desde el proyecto fuente
    features = client.query('SELECT * FROM `{t}`'.format(t=features_table)).to_dataframe()
    features = features['string_field_0'].tolist()

    X = X[features]

    def generate_datetime_created():
        return datetime.now()

    def generate_date_created():
        return datetime.now(TZ).date().strftime(FORMAT_DATE)

    def load_model_from_gcs(path_model):
        storage_client = storage.Client()
        bucket_name, blob_name = path_model.replace('gs://', '').split('/', 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        model_bytes = blob.download_as_string()
        return load(BytesIO(model_bytes))

    classifier = load_model_from_gcs(path_model)

    # El modelo devuelve el logaritmo natural del precio de venta; se guarda la
    # salida cruda antes de truncarla para poder reconstruir el precio.
    raw = classifier.predict(X)
    predictions = pd.DataFrame(raw, columns=['prediction'])

    user_id = client.query('SELECT SESSION_USER()').to_dataframe().iloc[0, 0]

    start_time = generate_datetime_created()
    execute_date = generate_date_created()

    predictions['prediction'] = predictions['prediction'].astype('int64')
    predictions['creation_user'] = user_id
    predictions['process_date'] = datetime.strptime(execute_date, '%Y-%m-%d')
    predictions['process_date'] = pd.to_datetime(predictions['process_date']).dt.date
    predictions['load_date'] = pd.to_datetime(start_time)

    # Se escribe en el proyecto de PRODUCCION, distinto al de procesamiento
    pandas_gbq.to_gbq(predictions, table_id, if_exists='append', project_id=project)

    print('Predicciones generadas: %d filas -> %s' % (len(predictions), table_id))

    # Foto del universo puntuado. Se arma en memoria, donde el orden de las
    # filas si esta garantizado, y se escribe como una sola fila ancha: por eso
    # no hace falta ninguna llave para relacionar la entrada con su resultado.
    universo = X.reset_index(drop=True).copy()
    universo['prediction'] = predictions['prediction']
    universo['prediction_log'] = raw
    universo['sale_price_usd'] = np.exp(raw)
    universo['input_overall_qual'] = input_overall_qual
    universo['creation_user'] = predictions['creation_user']
    universo['process_date'] = predictions['process_date']
    universo['load_date'] = predictions['load_date']

    pandas_gbq.to_gbq(universo, table_aux, if_exists='append', project_id=project)

    print('Universo archivado: %d filas x %d columnas -> %s'
          % (universo.shape[0], universo.shape[1], table_aux))


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------
@dsl.pipeline(
    name='pipeline-prediccion-proyecto-final',
    description='Pipeline de prediccion sobre tres proyectos GCP',
    pipeline_root='gs://%s/demo' % BUCKET,
)
def main_pipeline(
    project: str,
    source_input_table: str,
    features_table: str,
    table_output: str,
    table_id: str,
    table_aux: str,
    path_model: str,
    recipients: List[str],
    prc_preparar: str = PRC_PREPARAR,
    input_overall_qual: float = 0.0,
):
    notify_email_task = VertexNotificationEmailOp(recipients=recipients)
    notify_email_task.set_display_name('Notification Email')

    with dsl.ExitHandler(notify_email_task, name='Execute pipeline prediction'):

        validate_tables_job = validate_data(
            source_input_table=source_input_table,
            features_table=features_table,
        )
        validate_tables_job.set_display_name('Validate Data')

        with dsl.If(validate_tables_job.outputs['condition'] == 'false', name='no-execute'):
            error_op(msg='No se logro validar las tablas de ingesta del proyecto fuente.')

        with dsl.If(validate_tables_job.outputs['condition'] == 'true', name='execute'):
            # Componente 1: stored procedure en BigQuery
            query_task = build_query(
                prc_preparar=prc_preparar,
                input_overall_qual=input_overall_qual,
            )
            query_task.set_display_name('Build Query')

            bq_prc_1_op = BigqueryQueryJobOp(
                query=query_task.output,
                project=project,
                location='US',
                labels=PIPELINE_LABEL,
            )
            bq_prc_1_op.set_display_name('PRC_PREPARAR_UNIVERSO')

            # Componente 2: prediccion del modelo
            prediction_task = prediction(
                project=project,
                source_table=table_output,
                features_table=features_table,
                table_id=table_id,
                table_aux=table_aux,
                path_model=path_model,
                input_overall_qual=input_overall_qual,
            )
            prediction_task.set_display_name('PREDICTION_MODEL')
            prediction_task.after(bq_prc_1_op)


if __name__ == '__main__':
    salida = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipeline_prediction.yaml')
    compiler.Compiler().compile(
        pipeline_func=main_pipeline,
        package_path=salida,
    )
    print('Pipeline compilado en', salida)
