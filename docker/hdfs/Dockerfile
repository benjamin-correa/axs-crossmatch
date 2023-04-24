# Building on top of ubuntu
FROM openjdk:8-slim

ARG HADOOP_VERSION=3.2.4
# Refresh package lists
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y upgrade
RUN apt install -y arp-scan python3 curl nano

WORKDIR /opt



#to add java to the path
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$PATH:$JAVA_HOME:$JAVA_HOME/bin 

# Install Hadoop
RUN curl -L https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -s -o - | tar -xzf -
RUN mv hadoop-$HADOOP_VERSION hadoop


# Setup
WORKDIR /opt/hadoop
ENV PATH /opt/hadoop/bin:/opt/hadoop/sbin:$PATH
RUN sed --in-place='.ori' -e "s/\${JAVA_HOME}/\/usr\/lib\/jvm\/java-8-openjdk-amd64/" etc/hadoop/hadoop-env.sh


#to add Hadoop to the path
ENV HADOOP_HOME=/opt/hadoop/
ENV HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.4.jar
ENV PATH=$PATH:$HADOOP_HOME
ENV PATH=$PATH:$HADOOP_HOME/bin

ADD ./docker/hdfs/etc/hadoop/hadoop-env.sh $HADOOP_HOME/etc/hadoop/
ADD ./docker/hdfs/etc/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop/


