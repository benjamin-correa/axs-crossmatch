# Building on top of python 3.9
FROM python:3.9

# Add repository to install OpenJDK-8
RUN apt-get update && apt-get install -y \
    software-properties-common
RUN apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main'

# Install OpenJDK-8 & certificates
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Install Spark/Hadoop
RUN mkdir -p /opt/spark/
RUN apt-get install -y wget tar bash
RUN wget https://archive.apache.org/dist/spark/spark-3.0.2/spark-3.0.2-bin-hadoop3.2.tgz
RUN tar -xvzf spark-3.0.2-bin-hadoop3.2.tgz && \
    mv spark-3.0.2-bin-hadoop3.2/* /opt/spark/ && \
    rm -r spark-3.0.2-bin-hadoop3.2.tgz &&\
    rm -r spark-3.0.2-bin-hadoop3.2/

# Downloads Sedona-Python-Adapter, Geotools & Postgres jars
RUN mkdir -p /opt/spark/extra_jars/
RUN wget https://repo1.maven.org/maven2/org/apache/sedona/sedona-python-adapter-3.0_2.12/1.3.1-incubating/sedona-python-adapter-3.0_2.12-1.3.1-incubating.jar && mv sedona-python-adapter-3.0_2.12-1.3.1-incubating.jar /opt/spark/extra_jars/
RUN wget https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.3.0-27.2/geotools-wrapper-1.3.0-27.2.jar && mv geotools-wrapper-1.3.0-27.2.jar /opt/spark/extra_jars/
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.25.jre6.jar && mv postgresql-42.2.25.jre6.jar /opt/spark/extra_jars/

# Sets PATH
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV HADOOP_OPTS "$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"
ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
ENV PATH $PATH:$SPARK_HOME/bin
ENV PYTHONPATH $SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_PYTHON python3
ENV PATH $PATH:$JAVA_HOME/bin

# Exposing port 8888
EXPOSE 8888
