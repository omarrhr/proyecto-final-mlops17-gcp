# -*- coding: utf-8 -*-
"""
Cloud Run Job que lanza el pipeline de prediccion en Vertex AI.

Es el disparador del proceso batch: Cloud Scheduler ejecuta este job, el job
hace submit del PipelineJob y termina. No ejecuta la prediccion, solo la lanza,
por lo que su tiempo de vida es de segundos.

Toda la configuracion entra por variables de entorno, igual que en el material
de clase (Modulo 6, Sesion 3: despliegue_cloud_run/main.py), de modo que la
misma imagen sirve para desarrollo y produccion cambiando solo el despliegue.
"""
import os

from google.cloud import aiplatform


def run_pipeline():
    project = os.environ.get('PROJECT_ID')
    location = os.environ.get('LOCATION', 'us-central1')

    pipeline_path = os.environ.get('PIPELINE_PATH')
    model_path = os.environ.get('MODEL_PATH')
    service_account = os.environ.get('SERVICE_ACCOUNT')
    pipeline_root = os.environ.get('PIPELINE_ROOT')

    source_input_table = os.environ.get('SOURCE_INPUT_TABLE')
    features_table = os.environ.get('FEATURES_TABLE')
    table_output = os.environ.get('TABLE_OUTPUT')
    predictions_table = os.environ.get('PREDICTIONS_TABLE')
    aux_table = os.environ.get('AUX_TABLE')
    prc_preparar = os.environ.get('PRC_PREPARAR')
    overall_qual = float(os.environ.get('INPUT_OVERALL_QUAL', '0.0'))
    recipients = [c.strip() for c in os.environ.get('NOTIFICATION_EMAIL', '').split(',') if c.strip()]

    aiplatform.init(project=project, location=location)

    job = aiplatform.PipelineJob(
        display_name='prediccion-proyecto-final',
        template_path=pipeline_path,
        pipeline_root=pipeline_root,
        enable_caching=False,
        project=project,
        location=location,
        parameter_values={
            'project': project,
            'source_input_table': source_input_table,
            'features_table': features_table,
            'table_output': table_output,
            'table_id': predictions_table,
            'table_aux': aux_table,
            'path_model': model_path,
            'prc_preparar': prc_preparar,
            'input_overall_qual': overall_qual,
            'recipients': recipients,
        },
    )

    print('submit pipeline job ...')
    job.submit(service_account=service_account)
    print('lanzado:', job.resource_name)


if __name__ == '__main__':
    run_pipeline()
