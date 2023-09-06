export GOOGLE_APPLICATION_CREDENTIALS=./credentials/data-ingestion@study-gcp.json

install:
	pip install -r requirements.dev.txt


run_beam:
	python src/pipeline_beam.py --config-name=local_to_local --config-dir=/home/antcarlosd/Projects/study-apache-beam/jobs/beam/acnaweb

run_dataflow:
	python src/pipeline_dataflow.py --config-dir=/home/antcarlosd/Projects/study-apache-beam/jobs/dataflow/acnaweb --config-name=bucket_to_bucket

run_dataflow_bq:
	python src/pipeline_dataflow_bigquery.py --config-dir=/home/antcarlosd/Projects/study-apache-beam/jobs/dataflow/acnaweb --config-name=bucket_to_bigquery

show:
	python main.py --cfg hydra

publish:
	python src/publisher.py	+pubsub=producer

consume:
	python src/consumer.py	+pubsub=consumer