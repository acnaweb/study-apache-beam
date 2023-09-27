import logging
import hydra
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from omegaconf import DictConfig, OmegaConf
from statsd import StatsClient



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
            "setup_file": "./setup.py",
            "statsDHost": cfg.observability.statsDHost,
            "statsDPort": cfg.observability.statsDPort,
        }

class DhuoFlow:

    def __init__(self, cfg: DictConfig) -> None:
        self.cfg = cfg
        self.validate()

        self.statsClient = StatsClient(        
            host=self.cfg.observability.statsDHost, 
            port=self.cfg.observability.statsDPort, 
            prefix=self.cfg.observability.prefix
        )


    def send_metric(self, metric):
            logging.info(f"{self.cfg.job.name}.{metric}")
            self.statsClient.incr(f"{self.cfg.job.name}.{metric}")


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
        self.gcp_temp_location =  self.cfg.gcp.temp_location
        self.input_file = self.cfg.dataset.input.file
        self.input_separator = self.cfg.dataset.input.separator
        self.table_name = f"{self.gcp_project}:{self.cfg.dataset.output.table}"
        self.table_schema = DhuoFlowUtils.schema_as_line(self.cfg.dataset.output.schema)
        self.output_columns = self.cfg.dataset.output.columns
        self.subscription = self.cfg.gcp.pubsub.subscription

    def run(self) ->  None:

        statsClient = StatsClient(        
            host=self.cfg.observability.statsDHost, 
            port=self.cfg.observability.statsDPort, 
            prefix=self.cfg.observability.prefix
        )


        def send_metric(metric):
            logging.info(metric)
            statsClient.incr(metric)

        # check if use Dataflow
        use_dataflow = True
        if use_dataflow:
            pipelineOptions = PipelineOptions.from_dictionary(DhuoFlowUtils.build_dataflow_options(self.cfg))
        else:
            options = {           
                "streaming": True
            }
            pipelineOptions = PipelineOptions.from_dictionary(options) 
                     
        
        with beam.Pipeline(options = pipelineOptions) as p:   
            send_metric(f"pubsub.{self.subscription}")
            data = p | "Read topic" >> beam.io.ReadFromPubSub(subscription=self.subscription)
            data_as_list = data | "Split" >> beam.ParDo(SplitLines(self.input_separator))
            data_as_dict = DhuoFlowUtils.to_dict(data_as_list, self.output_columns)
            send_metric(f"save.bigquery.{self.table_name}")
            DhuoFlowUtils.save_bigquery(data_as_dict, self.table_name, self.table_schema, self.gcp_temp_location)



@hydra.main(version_base=None, config_name="config", config_path=".")
def main(cfg: DictConfig):

    print(OmegaConf.to_yaml(cfg))
    setup_logging()

    dhuoflow = DhuoFlow(cfg)
    dhuoflow.run()


if __name__ == "__main__":
    main()

    