FROM bitnami/spark:3.5.3

WORKDIR /app

USER root
RUN install_packages curl wget

# Baixa o conector do Google Cloud Storage para Hadoop 3.x
RUN wget -P /opt/bitnami/spark/jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# Baixa a versão adequada do Guava (caso necessário, substituindo a versão anterior)
RUN rm /opt/bitnami/spark/jars/guava-14.0.1.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar

# Copia o JAR local para o diretório de JARs do Spark dentro do container
COPY mnt/spark_job/jars/gcs-connector-hadoop3-2.2.2-shaded.jar /opt/bitnami/spark/jars/

# Altera permissões dos arquivos baixados
RUN chmod 644 /opt/bitnami/spark/jars/guava-23.0.jar
RUN chmod 644 /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar
RUN chmod 644 /opt/bitnami/spark/jars/gcs-connector-hadoop3-2.2.2-shaded.jar

# Configurar o classpath do Spark para incluir os JARs adicionais
ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*:/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar:/opt/bitnami/spark/jars/gcs-connector-hadoop3-2.2.2-shaded.jar"

# Define variáveis de ambiente para autenticação no GCS
ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/dags/credential/google_credential.json"

# Configurar permissões para a pasta de JARs do Spark
RUN chmod -R 777 /opt/bitnami/spark/jars

USER 1001
