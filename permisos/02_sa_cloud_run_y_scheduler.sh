#!/bin/bash
# SA del Cloud Run Job y del Scheduler.
# "La cuenta de servicio de Vertex debe ser impersonalizada con la cuenta de
#  servicio de Cloud Run Job" - Sesion 3 GCP III, 0:50:10
PROC=dp-mlops17-ohuaman-pf-proc
BUCKET=dp-mlops17-ohuaman-pf-vertex
SA_RUN="sa-cloud-run-job@${PROC}.iam.gserviceaccount.com"
SA_VERTEX="sa-vertex-pipeline@${PROC}.iam.gserviceaccount.com"
SA_SCH="sa-scheduler@${PROC}.iam.gserviceaccount.com"

gcloud iam service-accounts create sa-cloud-run-job --project="$PROC"
gcloud projects add-iam-policy-binding "$PROC" --member="serviceAccount:$SA_RUN" --role="roles/aiplatform.user" --condition=None
gcloud projects add-iam-policy-binding "$PROC" --member="serviceAccount:$SA_RUN" --role="roles/iam.serviceAccountTokenCreator" --condition=None
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" --member="serviceAccount:$SA_RUN" --role="roles/storage.objectViewer" --project="$PROC"
# SUPLANTACION
gcloud iam service-accounts add-iam-policy-binding "$SA_VERTEX" --project="$PROC" \
  --member="serviceAccount:$SA_RUN" --role="roles/iam.serviceAccountUser"

# Scheduler: permisos minimos. "El Scheduler solamente necesita gatillar
# un servicio y ya esta" - Sesion 3 GCP III, 1:18:23
gcloud iam service-accounts create sa-scheduler --project="$PROC"
gcloud run jobs add-iam-policy-binding run-pipeline-job --region=us-central1 --project="$PROC" \
  --member="serviceAccount:${SA_SCH}" --role="roles/run.invoker"
