export GOOGLE_APPLICATION_CREDENTIALS=/home/ac/Projects/study-apache-beam/credentials/sa-petrobras@dhuodata.json
install:
	python -m venv venv; \
	. venv/bin/activate; \
	pip install -r requirements.dev.txt; \

cs_to_cs:
	python src/pipeline/cs_to_cs/pipeline.py

cs_to_bq:
	python src/pipeline/cs_to_bq/pipeline.py	

cs_to_bt:
	python src/pipeline/cs_to_bt/pipeline.py	

ps_to_bq:
	python src/pipeline/ps_to_bq/pipeline.py		

ps_to_bt:
	python src/pipeline/ps_to_bt/pipeline.py	

build:
	docker build . --tag beam
