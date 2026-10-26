# Imagen del Cloud Run Job que lanza el pipeline de Vertex.
# El material de clase usa python:3.8 (~900 MB). Se usa la variante slim para
# no agotar la capa gratuita de Artifact Registry (0.5 GB) y acelerar el build.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY main.py .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]
