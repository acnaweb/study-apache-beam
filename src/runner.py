import importlib
import hydra
from omegaconf import DictConfig, OmegaConf
from data_ingestion import DataIngestion




@hydra.main(version_base=None)
def main(cfg : DictConfig):

    print(OmegaConf.to_yaml(cfg))

    import sys
    sys.path.append(cfg.pipeline.path)

    di = DataIngestion(cfg)

    pipeline_func = "pipeline"
    pipeline_alias = getattr(importlib.import_module(cfg.pipeline.name), pipeline_func)
    di.run(pipeline_alias)

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



    # if cfg.output.bigquery:
    #     output_bigquery = {        
    #         "table": cfg.output.bigquery.table,
    #         "schema": ",".join(cfg.output.bigquery.schema)
    #     }


    #     # Has Pubsub
    #     if cfg.get("pubsub"):
    #         pubsub = {
    #             "topic": cfg.pubsub.topic,
    #             "subscription": cfg.pubsub.subscription
    #         }
    #         data = p | "Read topic" >> beam.io.ReadFromPubSub(subscription=pubsub["subscription"])

    #     # data_window = data | "Window" >> beam.WindowInto(window.)


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

