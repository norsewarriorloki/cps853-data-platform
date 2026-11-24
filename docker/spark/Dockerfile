FROM spark:4.2.1-python3

USER root

WORKDIR /app

RUN python3 -m pip install --no-cache-dir \
    --target=/opt/python-deps \
    "psycopg[binary]"

ENV PYTHONPATH=/opt/python-deps

USER spark