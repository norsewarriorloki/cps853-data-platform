FROM apache/kafka:4.1.2

USER root

RUN mkdir -p /tmp/kraft-combined-logs \
    && chown -R 1000:1000 /tmp/kraft-combined-logs

USER appuser