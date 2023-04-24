FROM hdfs-namenode:latest
RUN mkdir /opt/hdfs/datanode
ADD ./docker/hdfs-datanode/etc/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
CMD hdfs datanode