FROM apache/spark:3.5.1

USER root

# --------------------------------------------------
# PostgreSQL JDBC Driver
# --------------------------------------------------

RUN apt-get update && \
    apt-get install -y wget && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -P /opt/spark/jars/ && \
    rm -rf /var/lib/apt/lists/*

# --------------------------------------------------
# Spark Ivy Cache
# --------------------------------------------------

RUN mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark

WORKDIR /app

USER spark