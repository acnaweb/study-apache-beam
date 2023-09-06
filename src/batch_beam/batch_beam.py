import hydra
import apache_beam as beam
from omegaconf import DictConfig
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


@hydra.main(version_base=None, config_path="../config")
def run(cfg : DictConfig):   
    pipelineOptions = PipelineOptions(argc=None)    

    input_file = {
        "file": cfg.inputs.voos.file,
        "separator": cfg.inputs.voos.separator
    }

    output_file = {
        "file": cfg.outputs.voos.file,        
        "header": cfg.outputs.voos.header
    }

    with beam.Pipeline(options = pipelineOptions) as p:       
        lines = p | "Load Data" >> beam.io.ReadFromText(input_file["file"]) 
        data_as_list = lines | "Split" >> beam.Map(lambda record: record.split(input_file["separator"]))
        output = data_as_list | "Save Output" >> beam.io.WriteToText(output_file["file"], header=output_file["header"])

if __name__ == "__main__":
    run()        
   