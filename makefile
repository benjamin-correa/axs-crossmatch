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
	@docker run --rm -it --name=sedona -v $(shell pwd):/home/axs-crossmatch --workdir /home/axs-crossmatch -p 8888:8888 sedona:latest /bin/bash

docker-build:
	@echo "Building Docker Image in $(shell pwd)"
	@docker build -t sedona .
