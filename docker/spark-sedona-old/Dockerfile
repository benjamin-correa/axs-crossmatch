# Building on top of python 3.9
# FROM python:3.9
FROM openjdk:8-slim
COPY --from=python:3.9 / /

# Refresh package lists
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y upgrade
RUN apt install -y arp-scan curl nano

#to add java to the path
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$PATH:$JAVA_HOME:$JAVA_HOME/bi


# Install Spark/Hadoop
RUN mkdir -p /opt/spark/
RUN apt-get install -y wget tar bash
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
RUN tar -xvzf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3/* /opt/spark/ && \
    rm -r spark-3.3.2-bin-hadoop3.tgz &&\
    rm -r spark-3.3.2-bin-hadoop3/

# Downloads Sedona-Python-Adapter, Geotools & Postgres jars
RUN mkdir -p /opt/spark/extra_jars/
RUN wget https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/1.4.0/sedona-spark-shaded-3.0_2.12-1.4.0.jar && mv sedona-spark-shaded-3.0_2.12-1.4.0.jar /opt/spark/extra_jars/
RUN wget https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.4.0-28.2/geotools-wrapper-1.4.0-28.2.jar && mv geotools-wrapper-1.4.0-28.2.jar /opt/spark/extra_jars/
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.25.jre6.jar && mv postgresql-42.2.25.jre6.jar /opt/spark/extra_jars/

# Sets PATH
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV HADOOP_OPTS "$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"
ENV PATH $PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:%SPARK_HOME%/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON python3
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/python:$PATH

# Exposing port 8080
EXPOSE 8080

WORKDIR /home/axs-crossmatch

# ENTRYPOINT [ "docker/spark-sedona/entrypoint.sh" ]
