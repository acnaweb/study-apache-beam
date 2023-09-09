import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from omegaconf import DictConfig
from utils import (load_file, save_file)


class FileToFile:

    def __init__(self, cfg : DictConfig) -> None:
        self.cfg = cfg
        self.validate()


    def validate(self) -> bool:
        if not self.cfg.dataset.input.file:
            raise Exception("cfg.dataset.input.file not found")
        elif not self.cfg.dataset.output.file:
            raise Exception("cfg.dataset.output.file not found")
        self.input_file = self.cfg.dataset.input.file
        self.output_file = self.cfg.dataset.output.file


    def get_dataflow_options(self):
        return  {
            "project": self.cfg.gcp.project,
            "runner": "DataflowRunner",
            "region": self.cfg.gcp.dataflow.region,
            "staging_location": self.cfg.gcp.dataflow.staging_location,
            "temp_location": self.cfg.gcp.temp_location,
            "template_location": self.cfg.gcp.dataflow.template_location,
            "save_main_session": self.cfg.gcp.dataflow.save_main_session,
            "subnetwork": self.cfg.gcp.dataflow.subnetwork,
            "requirements_file": self.cfg.gcp.dataflow.requirements_file,
            "streaming": self.cfg.gcp.dataflow.streaming,
            "max_num_workers": self.cfg.gcp.dataflow.max_num_workers
        }


    def run(self, callback) ->  None:
        # check if use Dataflow
        # if self.cfg.gcp:
        #     if self.cfg.gcp.dataflow: 
        #         pipelineOptions = PipelineOptions.from_dictionary(self.get_dataflow_options())
        # else:
        pipelineOptions = PipelineOptions(argc=None)    
            
        with beam.Pipeline(options = pipelineOptions) as p:   
            data_load_file = load_file(p, self.input_file)
            data_callback = callback(data_load_file, self.cfg)
            save_file(data_callback, self.output_file)
