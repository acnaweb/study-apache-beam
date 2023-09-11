export CONFIG_DIR_BASE=./pipelines
export GOOGLE_APPLICATION_CREDENTIALS=/home/ac/Projects/study-apache-beam/credentials/data-ingestion@study-gcp.json

install:
	python -m venv venv; \
	. venv/bin/activate; \
	pip install -r requirements.dev.txt; \

cs_to_cs:
	python src/pipeline/cs_to_cs.py --config-dir=${CONFIG_DIR_BASE} --config-name=cs_to_cs