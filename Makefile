export GOOGLE_APPLICATION_CREDENTIALS=/home/ac/Projects/study-apache-beam/credentials/data-ingestion@study-gcp.json

install:
	python -m venv venv; \
	. venv/bin/activate; \
	pip install -r requirements.dev.txt; \

cs_to_cs:
	python src/pipeline/cs_to_cs/pipeline.py

cs_to_bq:
	python src/pipeline/cs_to_bq/pipeline.py	