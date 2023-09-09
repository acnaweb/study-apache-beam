import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from omegaconf import DictConfig
from utils import (load_file, save_bigquery)
from dataflow import build_dataflow_options


class FileToBigQuery:

    def __init__(self, cfg : DictConfig) -> None:
        self.cfg = cfg
        self.validate()


    def schema_as_line(self, columns) -> str:     
        return ",".join(columns)


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
        self.table_name = f"{self.gcp_project}:{self.cfg.dataset.output.table}"
        self.table_schema = self.schema_as_line(self.cfg.dataset.output.schema)
        

    def run(self, callback) ->  None:
        # check if use Dataflow
        if self.cfg.gcp.dataflow: 
            pipelineOptions = PipelineOptions.from_dictionary(build_dataflow_options(self.cfg))
        else:
            pipelineOptions = PipelineOptions(argc=None)    

        with beam.Pipeline(options = pipelineOptions) as p:   
            data_load_file = load_file(p, self.input_file)
            data_callback = callback(data_load_file, self.cfg)
            save_bigquery(data_callback, self.table_name, self.table_schema, self.gcp_temp_location)
