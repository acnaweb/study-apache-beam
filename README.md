# Study Apache Beam


## Features

| Config Template | Source (Input) | Target (Output) | Runner |
|---|---|---|---|
| file_to_file.yaml| File System | File System | Apache Beam |
| cs_to_cs.yaml| Cloud Storage | Cloud Storage | Apache Beam |


### Environment


#### Python (Apache Beam credentials)

```
export GOOGLE_APPLICATION_CREDENTIALS=/path/google_credentials.json
```
or 

.env
```
GOOGLE_APPLICATION_CREDENTIALS=/path/google_credentials.json
```

#### Terraform credentials

deploy/terraform.tfvars
```
project_id="study-gcp-398200"
region="us-west1"
credentials="../credentials/data-ingestion@study-gcp.json"
prefix="prefix"
```

#### Terraform apply/destroy

terraform destroy -var-file="terraform.acnaweb.tfvars"


### Object Store

```
bucket_name/inputs
bucket_name/outputs
bucket_name/temp
bucket_name/template
```

### Datasets

- https://caelum-online-public.s3.amazonaws.com/1954-apachebeam/alura-apachebeam-basedados.rar

- https://caelum-online-public.s3.amazonaws.com/1954-apachebeam/alura-apachebeam-sampledados.rar

### Issues

- Como paralelizar?

## Referencias

- https://beam.apache.org/documentation/patterns/pipeline-options/
- https://beam.apache.org/documentation/transforms/python/elementwise/map/
- https://beam.apache.org/documentation/programming-guide/#pcollection-characteristics
- https://python.plainenglish.io/apache-beam-flink-cluster-kubernetes-python-a1965f37b7cb
- https://towardsdatascience.com/data-pipelines-with-apache-beam-86cd8eb55fd8
- https://blog.marcnuri.com/prometheus-grafana-setup-minikube
- https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
- https://itnext.io/pubsub-to-bigtable-piping-your-data-stream-in-via-gcp-cloud-functions-a2ef785935b5
- https://skuad-engineering.medium.com/apache-beam-etl-via-google-cloud-dataflow-from-bigquery-to-bigtable-59665a5dfd70
- https://www.macrometa.com/event-stream-processing/apache-beam-tutorial
- https://beam.apache.org/documentation/transforms/python/aggregation/sample/
- https://beam.apache.org/documentation/pipelines/design-your-pipeline/
- https://cloud.google.com/dataflow/docs/guides/using-custom-containers
- https://medium.com/google-cloud/installing-python-dependencies-in-dataflow-fe1c6cf57784
- https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
- https://godatadriven.com/blog/a-practical-guide-to-using-setup-py/
- https://cloud.google.com/dataflow/docs/tutorials/dataflow-stream-to-bigquery?hl=pt-br
- https://itnext.io/pubsub-to-bigtable-piping-your-data-stream-in-via-gcp-cloud-functions-a2ef785935b5
- https://stackoverflow.com/questions/71438134/streaming-pubsub-bigtable-using-apache-beam-dataflow-java
- https://www.cloudskillsboost.google/focuses/58496?parent=catalog
- https://cloud.google.com/bigtable/docs/cbt-overview?hl=pt-br
- https://cloud.google.com/sdk/gcloud/reference/auth/login
- https://cloud.google.com/bigtable/docs/cbt-overview?hl=pt-br
- https://cloud.google.com/sdk/docs/quickstart-linux
- https://www.kamalsblog.com/2019/10/moving-data-from-google-storage-to-bigtable-using-dataflow.html
- https://cloud.google.com/bigtable/docs/samples/bigtable-writes-batch?hl=pt-br
- https://medium.com/@Sushil_Kumar/-custom-metrics-in-dataflow-pipelines-with-prometheus-and-statsd-9f37332f2992
- https://dev.to/kirklewis/metrics-with-prometheus-statsd-exporter-and-grafana-5145
- https://pypi.org/project/statsd-exporter/
- https://github.com/prometheus/statsd_exporter

### Terraform

- https://github.com/terraform-google-modules/terraform-google-cloud-storage/tree/v4.0.1/modules/simple_bucket

### Create Data flow Job
```
POST /v1b3/projects/${{PROJECT_ID}}/locations/${{REGION}}/templates:launch?gcsPath=${GCP_TEMP_LOCATION}}/${{JOB_NAME}}
{
    "jobName": "job_base",
    "environment": {
        "bypassTempDirValidation": false,
        "tempLocation": "gs://prefix-datalake-demo/temp/",
        "additionalExperiments": [],
        "additionalUserLabels": {}
    },
    "parameters": {}
}
```