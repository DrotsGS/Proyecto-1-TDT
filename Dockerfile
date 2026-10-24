FROM python:3.12-slim
# Sin esto Python guarda la salida en un buffer y "docker logs" no
# muestra nada hasta que el proceso termina.
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN python scripts/gen_proto.py
