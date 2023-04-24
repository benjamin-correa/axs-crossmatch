# @python src/pipelines/pre_processing/catwise/join_tables.py
test:
	@python src/pipelines/query/catwise/test.py
	@python src/pipelines/query/catwise/query_data.py

filter-columns:
	@find data/02_intermediate/** -empty -type d -delete
	@python src/pipelines/pre_processing/catwise/filter_columns_spark.py


join-tables:
	@python src/pipelines/pre_processing/catwise/join_tables.py

convert-csv:
	@python src/pipelines/pre_processing/catwise/convert_to_spark_csv.py

download:
	@python src/pipelines/pre_processing/catwise/download.py

install:
	pip install -r requirements.txt

pre-processing:
	@python src/pipelines/pre_processing/catwise/download.py
	@find data/02_intermediate/** -empty -type d -delete
	@python src/pipelines/pre_processing/catwise/filter_columns_spark.py

docker-run:
	@echo "Running docker container"
	@-docker container rm -f sedona
	@docker run --rm -it --name=sedona -v $(shell pwd):/home/axs-crossmatch --workdir /home/axs-crossmatch -p 8080:8080 sedona:latest bash
docker-build:
	@echo "Building Docker Image in $(shell pwd)"
	@docker build -t sedona . -f docker/spark-sedona/Dockerfile
	@docker build -t hdfs . -f docker/hdfs/Dockerfile
	@docker build -t hdfs-namenode . -f docker/hdfs-namenode/Dockerfile
	@docker build -t hdfs-datanode . -f docker/hdfs-datanode/Dockerfile


docker-test:
	@docker run --rm -it --name=hdfs -v $(shell pwd):/home/axs-crossmatch -p 22022:22 -p 8020:8020 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 --workdir /home hdfs:latest bash
	docker run -it --net dock_net --name hdfs-datanode-use -v $(shell pwd):/home/axs-crossmatch hdfs-datanode:latest /bin/bash

docker-hdfs:
	@-docker container rm -f hdfs-namenode
	@-docker container rm -f hdfs-datanode1
	@-docker container rm -f hdfs-datanade2
	@-docker container rm -f hdfs-datanode3
	@-docker container rm -f hdfs-datanode-use
	docker run -d --net dock_net --hostname namenode-master -p 9870:9870 -p 50030:50030 --name hdfs-namenode hdfs-namenode:latest
	docker run -d --net dock_net --name hdfs-datanode1 hdfs-datanode:latest
	docker run -d --net dock_net --name hdfs-datanade2 hdfs-datanode:latest
	docker run -d --net dock_net --name hdfs-datanode3 hdfs-datanode:latest
	docker run --rm --net dock_net -it --name=sedona -v $(shell pwd):/home/axs-crossmatch --workdir /home/axs-crossmatch -p 8080:8080 sedona:latest bash