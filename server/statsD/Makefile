network:
	docker network create -d bridge metrics-bridge

run:
	docker run -d --name prom-statsd-exporter \
 	--network metrics-bridge \
	-p 9102:9102 \
	-p 9125:9125 \
	-p 9125:9125/udp \
	-v ./statsd_mapping.yml:/tmp/statsd_mapping.yml \
	prom/statsd-exporter --statsd.mapping-config=/tmp/statsd_mapping.yml	

send_metrics:
	./send_metric.sh	


	