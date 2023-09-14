run:
	docker run -d --name prometheus \
	--network metrics-bridge \
	-p 9090:9090 \
	-v ./prometheus.yml:/tmp/prometheus.yml \
	prom/prometheus --config.file=/tmp/prometheus.yml --web.listen-address '0.0.0.0:9090'