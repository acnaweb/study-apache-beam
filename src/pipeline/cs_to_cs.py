import hydra
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from omegaconf import DictConfig, OmegaConf


def to_list(data, separator):
    data_split = data | "Split" >> beam.Map(lambda record: record)
    return data_split


def to_dict(record, columns):
    return dict(zip(columns, record))


def load_file(pipeline, file):
    return pipeline | "Load file " >> beam.io.ReadFromText(file) 


def save_file(data, file):
    return data | "Save file " >> beam.io.WriteToText(file)


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
        elif not self.cfg.dataset.output.file:
            raise Exception("cfg.dataset.output.file not found")
        self.input_file = self.cfg.dataset.input.file
        self.input_separator = self.cfg.dataset.input.separator
        self.output_file = self.cfg.dataset.output.file
        self.output_columns = self.cfg.dataset.output.columns

    def run(self) ->  None:
        # check if use Dataflow
        if self.cfg.gcp.dataflow:
            pipelineOptions = PipelineOptions.from_dictionary(build_dataflow_options(self.cfg))
        else:
            pipelineOptions = PipelineOptions(argc=None)    
            
        with beam.Pipeline(options = pipelineOptions) as p:   
            data = load_file(p, self.input_file)            
            data_as_list = to_list(data, self.output_columns)
            # data_as_dict = to_dict(data_as_list, self.output_columns)
            save_file(data_as_list, self.output_file)


@hydra.main(version_base=None)
def main(cfg: DictConfig):

    print(OmegaConf.to_yaml(cfg))

    dhuoflow = DhuoFlow(cfg)
    dhuoflow.run()

if __name__ == "__main__":
    main()

    