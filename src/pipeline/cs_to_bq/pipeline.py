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
            "setup_file": "./setup.py"
        }



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
        if self.cfg.use_dataflow:
            pipelineOptions = PipelineOptions.from_dictionary(DhuoFlowUtils.build_dataflow_options(self.cfg))
        else:
            pipelineOptions = PipelineOptions(argc=None)    
            
        with beam.Pipeline(options = pipelineOptions) as p:   
            data = DhuoFlowUtils.load_file(p, self.input_file)            
            data_as_list = DhuoFlowUtils.to_list(data, self.input_separator)
            data_as_dict = DhuoFlowUtils.to_dict(data_as_list, self.output_columns)
            # data_as_list = data | "Split" >> beam.Map(lambda record: record.split(self.input_separator))
            # data_as_dict = data_as_list | "List To Dict" >> beam.Map(list_to_dict, self.output_columns)
            DhuoFlowUtils.save_bigquery(data_as_dict, self.table_name, self.table_schema, self.gcp_temp_location)

    

@hydra.main(version_base=None, config_name="config", config_path=".")
def main(cfg: DictConfig):

    print(OmegaConf.to_yaml(cfg))

    dhuoflow = DhuoFlow(cfg)
    dhuoflow.run()


if __name__ == "__main__":
    main()

    