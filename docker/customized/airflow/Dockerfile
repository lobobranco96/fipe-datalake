FROM apache/airflow:latest-python3.11

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV HADOOP_HOME /opt/hadoop-3.3.6
ENV HADOOP_COMMON_LIB_NATIVE_DIR /opt/hadoop-3.3.6/lib/native
ENV HADOOP_OPTS " -Djava.library.path=/opt/hadoop-3.3.6/lib/native"
ENV LD_LIBRARY_PATH /opt/hadoop-3.3.6/lib/native

USER root

RUN set -eux; \
    apt-get update && \
    apt-get install --yes --no-install-recommends \
    wget \
    openjdk-17-jdk


WORKDIR /opt
USER airflow


COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

#COPY mnt/dags /opt/airflow/dags
# COPY mnt/airflow.cfg /opt/airflow/airflow.cfg