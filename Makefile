
export CONFIG_DIR_BASE=/home/ac/Projects/study-apache-beam/config


install:
	pip install -r requirements.dev.txt


run_beam:
	python src/pipeline_beam.py --config-dir=/home/ac/Projects/study-apache-beam/jobs/beam/acnaweb --config-name=local_to_local 

run_dataflow:
	python src/pipeline_dataflow.py --config-dir=/home/ac/Projects/study-apache-beam/jobs/dataflow/acnaweb --config-name=bucket_to_bucket

run_dataflow_bq:
	python src/pipeline_dataflow_bigquery.py --config-dir=/home/ac/Projects/study-apache-beam/jobs/dataflow/acnaweb --config-name=bucket_to_bigquery

show:
	python main.py --cfg hydra

produce:
	python src/producer.py	--config-dir=${CONFIG_DIR_BASE}/dataflow/acnaweb --config-name=all
	
consume:
	python src/consumer.py	--config-dir=${CONFIG_DIR_BASE}/dataflow/acnaweb --config-name=all

file_to_file:
	python src/data_ingestion.py --config-dir=${CONFIG_DIR_BASE} --config-name=file_to_file

cs_to_cs:
	export GOOGLE_APPLICATION_CREDENTIALS=./credentials/data-ingestion@study-gcp.json && \
	python src/data_ingestion.py --config-dir=${CONFIG_DIR_BASE} --config-name=cs_to_cs