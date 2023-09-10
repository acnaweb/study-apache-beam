export CONFIG_DIR_BASE=./jobs
export GOOGLE_APPLICATION_CREDENTIALS=/home/antcarlosd/Projects/study-apache-beam/credentials/data-ingestion@study-gcp.json

install:
	python -m venv venv; \
	. venv/bin/activate; \
	pip install -r requirements.dev.txt; \

show:
	python main.py --cfg hydra

produce:
	python src/producer.py	--config-dir=${CONFIG_DIR_BASE}/dataflow/acnaweb --config-name=all
	
consume:
	python src/consumer.py	--config-dir=${CONFIG_DIR_BASE}/dataflow/acnaweb --config-name=all

beam_file_to_file:
	python src/main.py --config-dir=${CONFIG_DIR_BASE} --config-name=beam_file_to_file

beam_cs_to_cs:
	python src/main.py --config-dir=${CONFIG_DIR_BASE} --config-name=beam_cs_to_cs

beam_cs_to_bigquery:
	python src/main.py --config-dir=${CONFIG_DIR_BASE} --config-name=beam_cs_to_bigquery

dataflow_cs_to_cs:
	python src/main.py --config-dir=${CONFIG_DIR_BASE} --config-name=dataflow_cs_to_cs	