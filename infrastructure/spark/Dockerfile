FROM apache/spark:3.5.1

USER root

# Create Spark home directory
RUN mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark

WORKDIR /app

USER spark