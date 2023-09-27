### Criação de jobs


#### Path para upload de config.yaml

```
gs://petrobras-jobs-pubsub/jobs
```

#### Config.yaml 

```
job:  
  name: stream-ingestão-demo # nome do job no DataFlow (sem espaços ou caracteres especiais)
  type: STREAM | BATCH   
    # STREAM - lê tópico e faz ingestão em BigTable, salvando arquivos JSON em camada intermediária
    # BATCH - lê arquivos JSON da camada intermediária e salva no BigQuery

gcp:
  project: dhuodata
  dataflow:  
    region: us-west1 # region no GCP
    bucket_base: gs://petrobras-jobs-pubsub  # bucket base para upload do template do Dataflow    
    subnetwork: https://www.googleapis.com/compute/v1/projects/dhuodata/regions/us-west1/subnetworks/dhuodata-subnet-stg-cluster
    
  pubsub: # Se job.type = STREAM
    topic: projects/dhuodata/topics/dry-run-integra
    subscription: projects/dhuodata/subscriptions/dry-run-integra-sub
    
  bigtable: # Se job.type = STREAM
    instance: "bigtable_instance" 
    table: "table_name" 
    key: "row1" # Nome da key para o BigTable (deixaremos fixo para Petro)
    family_columns: "input" # Nome da family_column para o BigTable (deixaremos fixo para Petro)
    
  bigquery: # Se job.type = BATCH
    table: bigtable_dataset.table_name
    # Nota: o schema será inferido
```

