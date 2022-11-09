filter-columns:
	python src/pipelines/pre_processing/catwise/filter_columns.py

pre-processing:
	python src/pipelines/pre_processing/catwise/download.py
	python src/pipelines/pre_processing/catwise/filter_columns.py
