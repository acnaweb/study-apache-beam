export GOOGLE_APPLICATION_CREDENTIALS=./credentials/sa-petrobras@dhuodata.json

install:
	pip install -r requirements.dev.txt

run_dataflow:
	python src/batch_dataflow/job.py --config-dir=/home/ac/Projects/study-apache-beam/jobs/dataflow --config-name=ingest_cs_to_cs

show:
	python main.py --cfg hydra

publish:
	python src/publisher.py	+pubsub=producer

consume:
	python src/consumer.py	+pubsub=consumer