import hydra
import apache_beam as beam
from omegaconf import DictConfig, OmegaConf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

@hydra.main(version_base=None)
def run(cfg : DictConfig):

    print(OmegaConf.to_yaml(cfg))

    job_template = {
        "project": cfg.dataflow.project,
        "runner": cfg.dataflow.runner,
        "region": cfg.dataflow.region,
        "staging_location": cfg.dataflow.staging_location,
        "temp_location": cfg.dataflow.temp_location,
        "template_location": cfg.dataflow.template_location,
        "save_main_session": cfg.dataflow.save_main_session,
        "subnetwork": cfg.dataflow.subnetwork,
        "requirements_file": cfg.dataflow.requirements_file
    }

    # pipelineOptions = PipelineOptions(argc=None)
    pipelineOptions = PipelineOptions.from_dictionary(job_template)

    # Sets the requirements.txt file
    # pipelineOptions.view_as(SetupOptions).requirements_file = "requirements.txt"

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

