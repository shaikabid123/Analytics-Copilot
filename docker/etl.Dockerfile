# Spark runs in local mode inside this container. Java + the Postgres JDBC driver come from Debian packages
# (driver lands at /usr/share/java/postgresql.jar, which etl/common.py expects).
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/work:/work/backend
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless libpostgresql-jdbc-java \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY etl/requirements.txt /tmp/etl-requirements.txt
RUN pip install --no-cache-dir -r /tmp/etl-requirements.txt
CMD ["python", "-m", "etl.runner", "--help"]
