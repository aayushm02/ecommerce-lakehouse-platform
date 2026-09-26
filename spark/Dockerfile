FROM bitnami/spark:3.5.1

USER root
# Install necessary Python libraries for data processing
RUN pip install --no-cache-dir \
    minio \
    psycopg2-binary \
    pyarrow \
    pandas

USER 1001
