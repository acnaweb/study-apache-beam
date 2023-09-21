import logging
import hydra
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from omegaconf import DictConfig, OmegaConf
from statsd import StatsClient

# Imports the Cloud Logging client library
import google.cloud.logging


METRIC_PREFIX = f"ps_to_bt."

def setup_logging():
    LOG_FORMAT='%(asctime)s %(message)s'
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


class SplitLines(beam.DoFn):
  def __init__(self, separator):
      self.separator = separator
  
  def process(self,record):
    return [record.decode("utf-8").split(self.separator)]


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
                    schema=schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=gcs_temp_location
                )

    @staticmethod
    def build_dataflow_options(cfg : DictConfig, use_dataflow):
        return  {
            "job_name": cfg.job.name,
            "project": cfg.gcp.project,
            "runner": "DataflowRunner" if use_dataflow else "DirectRunner",
            "region": "us-west1",
            "staging_location": "gs://petrobras-jobs-pubsub/temp",
            "temp_location": "gs://petrobras-jobs-pubsub/temp",
            "template_location": f"gs://petrobras-jobs-pubsub/template/{cfg.job.name}",
            "save_main_session": True,
            "subnetwork": "https://www.googleapis.com/compute/v1/projects/dhuodata/regions/us-west1/subnetworks/dhuodata-subnet-stg-cluster",
            "streaming": True,
            # "max_num_workers": cfg.gcp.dataflow.max_num_workers,
            "setup_file": "./setup.py",
            "statsDHost": cfg.observability.statsDHost,
            "statsDPort": cfg.observability.statsDPort,
            # "sdk_container_image": "beam:latest"
            # "no_use_public_ips": True,
            # "requirements_file="/content/requirements.txt",
            # "options": ["--worker_disk_type=pd-ssd" , "--worker_disk_size_gb=10" , "--worker_machine_type=n1-standard-1" , "--num_workers=1" ,"--max_num_workers=3"]

        }
    

class ReadFromPubSub(beam.DoFn):

    def __init__(self, subscription):
        self.subscription = subscription


    def setup(self):
        host_statsd = "34.168.12.193"
        self.statsClient = StatsClient(        
            host=host_statsd, 
            port=9125, 
            prefix="statsd_dataflow"
        )


    def send_metric(self, metric):
        msg = f"{METRIC_PREFIX}{metric}"
        logging.info(msg)
        self.statsClient.incr(msg)        


    def process(self, input):
        self.send_metric(f"topic.{self.subscription}")
        output =  input | beam.io.ReadFromPubSub(subscription=f"{self.subscription}").with_output_types(bytes) 
        return output
    

class WriteToBigTable(beam.DoFn):

    def __init__(self, project, instance, table):
        self.project = project
        self.instance = instance
        self.table = table


    def setup(self):
        host_statsd = "34.168.12.193"
        self.statsClient = StatsClient(        
            host=host_statsd, 
            port=9125, 
            prefix="statsd_dataflow"
        )


    def send_metric(self, metric):
        msg = f"{METRIC_PREFIX}{metric}"
        logging.info(msg)
        self.statsClient.incr(msg)        


    def process(self, input):
        from apache_beam.io.gcp.bigtableio import WriteToBigTable

        self.send_metric(f"bigtable.{self.instance}.{self.table}")
        output = input | WriteToBigTable( project_id=self.project, 
                                                     instance_id=self.instance, 
                                                     table_id=self.tabled)    
        return output


class CreateInvoiceDescriptionRowFn(beam.DoFn):

    def __init__(self, instance_id, table_id, bucket, key, familia, subscription) :
        # # Instantiates a client
        # self.client = google.cloud.logging.Client()

        # # Retrieves a Cloud Logging handler based on the environment
        # # you're running in and integrates the handler with the
        # # Python logging module. By default this captures all logs
        # # at INFO level and higher
        # self.client.setup_logging()

        self.instance_id = instance_id
        self.table_id = table_id
        self.bucket = bucket
        self.key = key
        self.familia = familia
        self.subscription = subscription


    def setup(self):
        # host_statsd = "10.10.0.87"
        host_statsd = "34.168.12.193"

        self.statsClient = StatsClient(        
            host=host_statsd, 
            port=9125, 
            prefix="statsd_dataflow"
        )


    def send_metric(self, metric):

        msg = f"{METRIC_PREFIX}{metric}"
        logging.info(msg)
        self.statsClient.incr(msg)     

        msg = f"{METRIC_PREFIX}pubsub.{self.subscription}"
        logging.info(msg)
        self.statsClient.incr(msg)     



    def process(self, json_data):
        import datetime
        import json
        from apache_beam.io.gcp.gcsio import GcsIO
        from google.cloud.bigtable import row
        
        try:
            self.send_metric(f"bigtable.{self.table_id}")

            json_parsed = json_data.decode('utf-8').replace('\t', '').replace('\n', '').replace('\r', '')
            json_parsed = json.loads(json_parsed)

            # dt = datetime.date.today()
            # now = datetime.datetime.now()
            # dthm =('%02d:%02d.%d'%(now.minute,now.second,now.microsecond))[:-4]
            # with GcsIO().open(f"gs://{self.bucket}/messages/partition={dt}/{self.key}.json", 'w') as file:
            #     file.write(json.dumps(json_parsed).encode())            

            # Save message in processed folder in gcs bucket            
            direct_row = row.DirectRow(row_key=self.key.encode())
            for i, v in json_parsed.items():
                direct_row.set_cell(
                    self.familia,
                    str(i).encode(),
                    str(v).encode() if v is not None else ""
                )
            yield direct_row

        except Exception as e:
            direct_row = row.DirectRow(b'error')
            direct_row.set_cell(
                self.familia,
                b'Erro',
                b'erro_mensagem'
            )
            yield direct_row


class DhuoFlow:

    def __init__(self, cfg: DictConfig) -> None:
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


        self.gcp_project =  self.cfg.gcp.project
        self.gcp_temp_location =  "gs://petrobras-jobs-pubsub/temp"
        self.input_file = self.cfg.dataset.input.file
        self.input_separator = self.cfg.dataset.input.separator
        self.table_name = f"{self.gcp_project}:{self.cfg.dataset.output.table}"
        self.table_schema = DhuoFlowUtils.schema_as_line(self.cfg.dataset.output.schema)
        self.output_columns = self.cfg.dataset.output.columns
        self.subscription = self.cfg.gcp.pubsub.subscription

    def run(self) ->  None:

        # use Dataflow
        use_dataflow = False        
        pipelineOptions = PipelineOptions.from_dictionary(DhuoFlowUtils.build_dataflow_options(self.cfg, use_dataflow))            
        
        with beam.Pipeline(options=pipelineOptions) as p:
            from apache_beam.io.gcp.bigtableio import WriteToBigTable

            # data =  p | 'Read from Pub/Sub' >>  beam.ParDo(ReadFromPubSub(self.cfg.gcp.pubsub.subscription))   
            data =  p | beam.io.ReadFromPubSub(subscription=f"{self.cfg.gcp.pubsub.subscription}").with_output_types(bytes)

            data_as_row = data | 'Conversion string to row object' >> beam.ParDo(CreateInvoiceDescriptionRowFn(
                                        self.cfg.gcp.destination.instance,
                                        self.cfg.gcp.destination.table,
                                        "petrobras-teste",
                                        self.cfg.gcp.destination.key,
                                        self.cfg.gcp.destination.familia_columns,
                                        self.cfg.gcp.pubsub.subscription))
            
            # # data_as_row | 'Write to Bigtable' >> beam.ParDo(WriteToBigTable(self.cfg.gcp.project,
            # #                                                                 self.cfg.gcp.destination.instance,
            # #                                                                 self.cfg.gcp.destination.table)) 

            data_as_row | WriteToBigTable(project_id=self.cfg.gcp.project, 
                                                     instance_id=self.cfg.gcp.destination.instance, 
                                                     table_id=self.cfg.gcp.destination.table)                           

@hydra.main(version_base=None, config_name="config", config_path=".")
def main(cfg: DictConfig):

    print(OmegaConf.to_yaml(cfg))
    setup_logging()

    dhuoflow = DhuoFlow(cfg)
    dhuoflow.run()


if __name__ == "__main__":
    main()

    