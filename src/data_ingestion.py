import hydra
import apache_beam as beam
from omegaconf import DictConfig, OmegaConf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window


def load_file(pipeline, file):
    return pipeline | "Load file " >> beam.io.ReadFromText(file) 


def save_file(pcol, file):
    return pcol | "Save file " >> beam.io.WriteToText(file)

def list_to_dict(element, columns):
    return dict(zip(columns, element))


class FileSystemToFileSystem:
    def __init__(self, cfg : DictConfig) -> None:
        self.cfg = cfg
        self.validate()

    def validate(self) -> bool:
        if not self.cfg.dataset.input.file.name:
            raise Exception("cfg.dataset.input.file.name not found")
        elif not self.cfg.dataset.output.file.name:
            raise Exception("cfg.dataset.output.file.name not found")
        self.input_file = self.cfg.dataset.input.file.name
        self.output_file = self.cfg.dataset.output.file.name

    def run(self) ->  None:
        pipelineOptions = PipelineOptions(argc=None)    

        with beam.Pipeline(options = pipelineOptions) as p:   
            pcol_load_file = load_file(p, self.input_file)
            save_file(pcol_load_file, self.output_file)


class DhuoDataIngestion:

    def __init__(self, cfg : DictConfig) -> None:
        self.cfg = cfg
        self.job_type: str = self.cfg.job.type.upper()
        self.data_ingestion = None

        if self.job_type == "FS_FS":
            self.data_ingestion = FileSystemToFileSystem(self.cfg)

    def run(self) -> None:
        self.data_ingestion.run()
        

@hydra.main(version_base=None)
def main(cfg : DictConfig):

    print(OmegaConf.to_yaml(cfg))


    di = DhuoDataIngestion(cfg)
    di.run()

    # job_template = {}
    # # Is Apache Beam
    # if cfg.get("dataflow") == None:    
    #     job_template["streaming"] = True if cfg.get("pubsub") else False;
        
    # else:
    #     # Is DataFlow
    #     job_template = {
    #         "project": cfg.gcp.project,
    #         "runner": cfg.dataflow.runner,
    #         "region": cfg.dataflow.region,
    #         "staging_location": cfg.dataflow.staging_location,
    #         "temp_location": cfg.dataflow.temp_location,
    #         "template_location": cfg.dataflow.template_location,
    #         "save_main_session": cfg.dataflow.save_main_session,
    #         "subnetwork": cfg.dataflow.subnetwork,
    #         "requirements_file": cfg.dataflow.requirements_file,
    #         "streaming":  cfg.dataflow.streaming,
    #         "max_num_workers": cfg.dataflow.max_num_workers
    #     }
        
    # pipelineOptions = PipelineOptions.from_dictionary(job_template)

    # input_file = {
    #     "file": cfg.input.file,
    #     "separator": cfg.input.separator
    # }

    # # File System or Object Store
    # if cfg.output.file:
    #     output_file =  {
    #         "file": cfg.output.file.name,        
    #         "columns": cfg.output.file.columns        
    #     }

    # if cfg.output.bigquery:
    #     output_bigquery = {        
    #         "table": cfg.output.bigquery.table,
    #         "schema": ",".join(cfg.output.bigquery.schema)
    #     }

    # with beam.Pipeline(options = pipelineOptions) as p:   

    #     if cfg.get("dataflow") == None:
    #         data = p | "Load Data" >> beam.io.ReadFromText(input_file["file"]) 

    #     # Has Pubsub
    #     if cfg.get("pubsub"):
    #         pubsub = {
    #             "topic": cfg.pubsub.topic,
    #             "subscription": cfg.pubsub.subscription
    #         }
    #         data = p | "Read topic" >> beam.io.ReadFromPubSub(subscription=pubsub["subscription"])

    #     # data_window = data | "Window" >> beam.WindowInto(window.)
    #     # data_as_list = data | "Split" >> beam.Map(lambda record: record.split(input_file["separator"]))

    #     output = data | "Save Output" >> beam.io.WriteToText(output_file["file"])


    #     # as_dict  = as_list | "As dict" >> beam.Map(list_to_dict, output_file["columns"])
    #     # output = as_dict | "Write to Bigquery" >> beam.io.WriteToBigQuery(
    #     #             output_file["bigquery_table"],
    #     #             schema=output_file["bigquery_schema"],
    #     #             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #     #             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     #             custom_gcs_temp_location=job_template["temp_location"]
    #             # )

if __name__ == "__main__":
    main()

