import hydra
import apache_beam as beam
from omegaconf import DictConfig, OmegaConf
from apache_beam.options.pipeline_options import PipelineOptions

def list_to_dict(element, columns):
    return dict(zip(columns, element))


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

    input_file = {
        "file": cfg.inputs.voos.file,
        "separator": cfg.inputs.voos.separator
    }

    output_file = {
        "file": cfg.outputs.voos.file,        
        "columns": cfg.outputs.voos.columns,
        "bigquery_table": cfg.outputs.voos.bigquery.table,
        "bigquery_schema": ",".join(cfg.outputs.voos.bigquery.schema)
    }

    with beam.Pipeline(options = pipelineOptions) as p:       
        lines = p | "Load Data" >> beam.io.ReadFromText(input_file["file"]) 
        as_list = lines | "As List" >> beam.Map(lambda record: record.split(input_file["separator"]))
        as_dict  = as_list | "As dict" >> beam.Map(list_to_dict, output_file["columns"])
        output = as_dict | "Write to Bigquery" >> beam.io.WriteToBigQuery(
                    output_file["bigquery_table"],
                    schema=output_file["bigquery_schema"],
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=job_template["temp_location"]
                )

if __name__ == "__main__":
    run()

