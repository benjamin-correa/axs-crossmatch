# @python src/pipelines/pre_processing/catwise/join_tables.py
test:
	@python src/pipelines/query/catwise/test.py
	@python src/pipelines/query/catwise/query_data.py

create-parquet:
	@-rm -r data/04_query/catwise/catwise.parquet
	@python src/pipelines/query/catwise/create_parquet.py

create-geo-parquet:
	@-rm -r data/04_query/catwise/catwise_geohash.parquet
	@python src/pipelines/query/catwise/create_geo_parquet.py

create-equi-parquet:
	@python src/pipelines/query/catwise/create_parquet_equi_join.py

create-index:
	@python src/pipelines/query/catwise/create_index.py

filter-columns:
	@find data/02_intermediate/** -empty -type d -delete
	@python src/pipelines/pre_processing/catwise/filter_columns_spark.py

process-error-table:
	@python src/pipelines/pre_processing/catwise/process_error_table.py

create-all-equi-parquet:
	python3 src/pipelines/query/catwise/create_parquet_equi_join.py --cellid 4
	python3 src/pipelines/query/catwise/create_parquet_equi_join.py --cellid 9


create-coordinates:
	@-rm -r data/03_primary/catwise/catwise/
	@python src/pipelines/pre_processing/catwise/create_coordinates.py

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

docker-run-sedona:
	@echo "Running docker container"
	@-docker container rm -f sedona
	@docker run --net dock_net -it --name=sedona -v $(shell pwd):/home/axs-crossmatch --workdir /home/axs-crossmatch -p 8080:8080 sedona:latest bash
	# Test hdfs connection
	# telnet namenode-master 8020

docker-build-sedona:
	@echo "Building Docker Image in $(shell pwd)"
	@docker build -t sedona . -f docker/spark-sedona/Dockerfile

docker-build-hdfs:
	@docker build -t hdfs . -f docker/hdfs/Dockerfile
	@docker build -t hdfs-namenode . -f docker/hdfs-namenode/Dockerfile
	@docker build -t hdfs-datanode . -f docker/hdfs-datanode/Dockerfile

docker-run-hdfs:
	@-docker container rm -f hdfs-namenode
	@-docker container rm -f hdfs-datanode1
	@-docker container rm -f hdfs-datanade2
	@-docker container rm -f hdfs-datanode3
	docker run -d --net dock_net --hostname namenode-master -p 9870:9870 -p 50030:50030 --name hdfs-namenode -v $(shell pwd):/home/axs-crossmatch hdfs-namenode:latest
	docker run -d --net dock_net --name hdfs-datanode1 hdfs-datanode:latest
	# docker run -d --net dock_net --name hdfs-datanade2 hdfs-datanode:latest
	# docker run -d --net dock_net --name hdfs-datanode3 hdfs-datanode:latest 

hdfs-inspect:
	docker exec -it hdfs-namenode bash	

sedona-test:
	@docker run --rm --net dock_net -it --name=sedona_test -v $(shell pwd):/home/axs-crossmatch --workdir /home/axs-crossmatch -p 8081:8080 sedona:latest bash

docker-build-all:
	make docker-build-sedona
	make docker-build-hdfs

start-server-debug:
	uvicorn sedona_api:app --reload

start-server:
	uvicorn sedona_api:app