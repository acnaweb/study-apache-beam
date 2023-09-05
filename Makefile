install:
	pip install -r requirements.dev.txt

run_batch_local:
	python main.py +batch=voos_beam

run_a:
	python main.py +jobs=alura

show:
	python main.py --cfg hydra

publish:
	python src/publisher.py	+pubsub=producer

consume:
	python src/consumer.py	+pubsub=consumer
	export GOOGLE_APPLICATION_CREDENTIALS=/home/ac/Projects/study-apache-beam/credentials/study-beam-account.json