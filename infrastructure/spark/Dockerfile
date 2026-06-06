FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/spark/work-dir

# Kafka Connector
RUN curl -L \
https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
-o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar

RUN curl -L \
https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
-o /opt/spark/jars/kafka-clients-3.4.1.jar

WORKDIR /app

USER spark