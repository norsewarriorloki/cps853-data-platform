FROM spark:4.2.1-python3

USER root

WORKDIR /app

RUN python3 -m pip install --no-cache-dir \
    --target=/opt/python-deps \
    "psycopg[binary]"

ENV PYTHONPATH=/opt/python-deps
ENV PYTHONUNBUFFERED=1

COPY docker/spark/log4j2.properties /opt/spark/conf/log4j2.properties
COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
