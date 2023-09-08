
export CONFIG_DIR_BASE=/home/ac/Projects/study-apache-beam/config

install:
	pip install -r requirements.dev.txt

show:
	python main.py --cfg hydra

produce:
	python src/producer.py	--config-dir=${CONFIG_DIR_BASE}/dataflow/acnaweb --config-name=all
	
consume:
	python src/consumer.py	--config-dir=${CONFIG_DIR_BASE}/dataflow/acnaweb --config-name=all

file_to_file:
	python src/runner.py --config-dir=${CONFIG_DIR_BASE} --config-name=file_to_file

cs_to_cs:
	export GOOGLE_APPLICATION_CREDENTIALS=./credentials/data-ingestion@study-gcp.json && \
	python src/runner.py --config-dir=${CONFIG_DIR_BASE} --config-name=cs_to_cs