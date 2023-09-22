import hydra
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from omegaconf import DictConfig, OmegaConf


class DhuoFlowUtils:


    @staticmethod
    def list_to_dict(record, columns):
        return dict(zip(columns, record))


    @staticmethod
    def to_dict(data, columns):
        return data | "List To Dict" >> beam.Map(DhuoFlowUtils.list_to_dict, columns)


    @staticmethod
    def to_list(data, separator):
        data_split = data | "Split" >> beam.Map(lambda record: record.split(separator))
        return data_split


    @staticmethod
    def schema_as_line(columns) -> str:     
            return ",".join(columns)


    @staticmethod
    def load_file(pipeline, file):
        return pipeline | "Load file " >> beam.io.ReadFromText(file) 


    @staticmethod
    def save_file(data, file):
        return data | "Save file " >> beam.io.WriteToText(file)


    @staticmethod
    def save_bigquery(data, table_name, schema, gcs_temp_location):
        return data | "Write to Bigquery" >> beam.io.WriteToBigQuery(
                    table_name,
                    #schema=schema,
                    schema ='SCHEMA_AUTODETECT', 
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=gcs_temp_location
                )

    @staticmethod
    def build_dataflow_options(cfg : DictConfig):
        return  {
            "project": cfg.gcp.project,
            "runner": "DataflowRunner",
            "region": cfg.gcp.dataflow.region,
            "staging_location": cfg.gcp.dataflow.staging_location,
            "temp_location": cfg.gcp.temp_location,
            "template_location": f"{cfg.gcp.dataflow.template_location}/{cfg.job.name}",
            "save_main_session": cfg.gcp.dataflow.save_main_session,
            "subnetwork": cfg.gcp.dataflow.subnetwork,
            "streaming": cfg.gcp.dataflow.streaming,
            "max_num_workers": cfg.gcp.dataflow.max_num_workers,
            "setup_file": "./setup.py"
        }

def transf_to_json(json):
    for key, value in json.items():             
        json[key] = str(value)
    return json


class DhuoFlow:

    def __init__(self, cfg : DictConfig) -> None:
        self.cfg = cfg
        self.validate()

            
    def validate(self) -> bool:
        if not self.cfg.dataset.input.file:
            raise Exception("cfg.dataset.input.file not found")
        elif not self.cfg.job.name:
            raise Exception("cfg.job.name not found")
        elif not self.cfg.dataset.output.table:
            raise Exception("cfg.dataset.output.table not found")
        elif not self.cfg.dataset.output.schema:    
            raise Exception("cfg.dataset.output.schema not found")
        elif not self.cfg.gcp.project:
            raise Exception("cfg.gcp.project not found")
        elif not self.cfg.gcp.temp_location:
            raise Exception("cfg.gcp.temp_location not found")     

        self.gcp_project =  self.cfg.gcp.project
        self.gcp_temp_location =  self.cfg.gcp.temp_location
        self.input_file = self.cfg.dataset.input.file
        self.input_separator = self.cfg.dataset.input.separator
        self.table_name = f"{self.gcp_project}:{self.cfg.dataset.output.table}"
        self.table_schema = DhuoFlowUtils.schema_as_line(self.cfg.dataset.output.schema)
        self.output_columns = self.cfg.dataset.output.columns

    def run(self) ->  None:
        # check if use Dataflow
        use_dataflow = True
        if use_dataflow:
            pipelineOptions = PipelineOptions.from_dictionary(DhuoFlowUtils.build_dataflow_options(self.cfg))
        else:
            pipelineOptions = PipelineOptions(argc=None)    


        import os
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/ac/Projects/study-apache-beam/credentials/sa-petrobras@dhuodata.json'

        pipeline_options = PipelineOptions(
            job_name= self.cfg.job.name,
            runner= "DataflowRunner" ,
            region = "us-west1" ,
            temp_location = f"gs://petrobras-jobs-pubsub/template/{self.cfg.job.name}" ,
            # template_location = f"gs://petrobras-jobs-pubsub/template/{self.cfg.job.name}",
            project = "dhuodata" ,
            streaming = False,
            subnetwork = "https://www.googleapis.com/compute/v1/projects/dhuodata/regions/us-west1/subnetworks/dhuodata-subnet-stg-cluster",                        
            requirements_file= "./requirements.txt",
            options = [
                # "--job_name", self.cfg.job.name,
                "--worker_disk_type =pd-ssd" ,
                "--worker_disk_size_gb=10" ,
                "--worker_machine_type=n1-standard-1" ,
                "--num_workers=1" ,
                "--max_num_workers=3"
            ]
        )
            
        from google.oauth2 import service_account
        from google.cloud import storage
        import os
        import re
        import json
        import datetime

        # Instantiates a client
        storage_client = storage.Client.from_service_account_json(json_credentials_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

        # The name for the new bucket
        bucket_name = "petrobras-teste"

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(bucket_name)
        
        dt = str(datetime.date.today())
        now = datetime.datetime.now()
        dthm =('%02d:%02d.%d'%(now.minute,now.second,now.microsecond))[:-4]

        # Substitua 'YOUR_PROJECT_ID', 'YOUR_INSTANCE_ID', e 'YOUR_TABLE_NAME' pelos seus valores
        with beam.Pipeline(options = pipeline_options) as p:

            self.bucket = "petrobras-teste"
            # path = f"gs://{self.bucket}/messages/partition={dt}/*.json" #   2023-09-18/row1.json'

            path = 'gs://petrobras-teste/messages/partition=2023-09-21/17:22.78_row1.json'

            # path = f"gs://{bucket_name}/" + re.sub("\s+", "",txt.split(',')[-2])
            # with blob.open("r") as f:
            #     texto = f.read()
            #     print(texto)
            #     lines  =  p  | 'Getdata' >> beam.Create([texto])
                                        
            # TODO nome do job
            data = p |'Read from GCS' >> beam.io.ReadFromText(f'gs://petrobras-teste/messages/partition={dt}/*.json')

            # data = DhuoFlowUtils.load_file(p, path)  

            # data = p | "Load file " >> beam.io.ReadFromText(path)                    
            
            # data |  beam.Map(print)           
            
            # data  =  p  | 'Getdata' >> beam.Create([{"success": "true", "datatype": "float64", "timestamp": "1694802410052", "registerId": "943188A2-35C9-4E8C-B2A7-CEA4C543A94A", "value": "29.35", "deviceID": "2973506F-00D1-424C-ABB4-F7C6649C825E", "tagName": "tubesInc-area01-plc01-machineTemperature", "deviceName": "tubesInc-area01-plc01", "description": "Monitoring of machine temperature", "metadata": '{"rangeAlarmMax": "100", "rangeAlarmMin": "35"}' }]) 
            # data_as_list = DhuoFlowUtils.to_list(data, self.input_separator)
            # data_as_dict = DhuoFlowUtils.to_dict(data_as_list, self.output_columns)   

            data_parsed = data | "PARSE JSON" >> beam.Map(json.loads)

            data_x = data_parsed | "Convert " >> beam.Map(transf_to_json)

            # data_x | beam.Map(print)
                        
            DhuoFlowUtils.save_bigquery(data_x, self.table_name, self.table_schema, self.gcp_temp_location)

    
@hydra.main(version_base=None, config_name="config", config_path=".")
def main(cfg: DictConfig):

    print(OmegaConf.to_yaml(cfg))

    dhuoflow = DhuoFlow(cfg)
    dhuoflow.run()


if __name__ == "__main__":
    main()

    