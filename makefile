filter-columns:
	python src/pipelines/pre_processing/catwise/filter_columns_spark.py

download:
	python src/pipelines/pre_processing/catwise/download.py

pre-processing:
	python src/pipelines/pre_processing/catwise/download.py
	python src/pipelines/pre_processing/catwise/filter_columns_spark.py
