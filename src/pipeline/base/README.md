### Criação de jobs


#### Path para upload de config.yaml

```
gs://petrobras-jobs-pubsub/jobs
```

#### Config.yaml 

```
job:  
  name: stream-ingestão-demo # nome do job no DataFlow (sem espaços ou caracteres especiais)
  
gcp:
  pubsub: 
    subscription: projects/dhuodata/subscriptions/dry-run-integra-sub # subscription do stream
    windowing_interval: 10 # unidade de tempo em segundos 
    
  bigtable: 
    instance: "bigtable_instance" 
    table: "table_name" 
```

