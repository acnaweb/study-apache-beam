import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from omegaconf import DictConfig
from utils import (load_file, save_file)


class FileToFile:

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


    def run(self, callback) ->  None:
        pipelineOptions = PipelineOptions(argc=None)    

        with beam.Pipeline(options = pipelineOptions) as p:   
            data_load_file = load_file(p, self.input_file)
            data_callback = callback(data_load_file, self.cfg)
            save_file(data_callback, self.output_file)