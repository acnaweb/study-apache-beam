#!/bin/bash

echo -n 'statsd_dataflow.info.step_1.reason.missing_key:1|c' | nc -u -q0 localhost 9125
echo -n 'statsd_dataflow.info.step_1.reason.missing_key:1|c' | nc -u -q0 localhost 9125
echo -n 'statsd_dataflow.info.step_1.reason.missing_key:1|c' | nc -u -q0 localhost 9125
echo -n 'statsd_dataflow.info.step_1.reason.missing_key:1|c' | nc -u -q0 localhost 9125
echo -n 'statsd_dataflow.info.step_1.reason.missing_key:1|c' | nc -u -q0 localhost 9125

echo -n 'statsd_dataflow.pubsub-to-bigtable.pubsub.projects/dhuodata/topics/input-messages:1|c' | nc  -q0 localhost 35395