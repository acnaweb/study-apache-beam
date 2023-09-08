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
bucket_name="acnaweb-study-datalake"
credentials="../credentials/data-ingestion@study-gcp.json"
```

#### Terraform apply/destroy

terraform destroy -var-file="terraform.dhuodata.tfvars"


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


https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py

python3 -m \
    apache_beam.examples.wordcount \
    --region southamerica-east1 --input \
    gs://acnaweb-curso-apache-beam/inputs/voos_sample.csv \
    --output \
    gs://acnaweb-curso-apache-beam/outputs/voos.csv \
    --runner DataflowRunner \
    --project \
    curso-dataflow-beam-397504 \
    --temp_location \
    gs://acnaweb-curso-apache-beam/temp/


### Terraform

- https://github.com/terraform-google-modules/terraform-google-cloud-storage/tree/v4.0.1/modules/simple_bucket

