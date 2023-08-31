import hydra
import apache_beam as beam
from src.dengue import build_pipeline_dengue
from src.chuvas import build_pipeline_chuvas
from src.join_chuvas_dengue import build_pipeline_join_chuvas_dengue
from src.save import save
from omegaconf import DictConfig, OmegaConf
from apache_beam.options.pipeline_options import PipelineOptions

pipelineOptions = PipelineOptions(argc=None)

def pipeline_alura(cfg : DictConfig):
    pipeline: beam.Pipeline = beam.Pipeline(options = pipelineOptions)
    dengue = build_pipeline_dengue(pipeline, cfg.datasets.dengue)
    chuvas = build_pipeline_chuvas(pipeline, cfg.datasets.chuvas)
    dataset_chuvas_dengue = build_pipeline_join_chuvas_dengue(chuvas, dengue)
    save(dataset_chuvas_dengue, cfg.datasets.output)

    # run pipeline
    pipeline.run()


def pipeline_udemy(cfg : DictConfig):
    pipeline: beam.Pipeline = beam.Pipeline(options = pipelineOptions)
    voos = (
        pipeline
        | "Step1" >> beam.io.ReadFromText(cfg.datasets.poema.path)
        | "Split" >> beam.Map(lambda record: record.split(cfg.datasets.poema.separator))
        | "Save" >> beam.io.WriteToText(file_path_prefix=cfg.datasets.output.path,
                                        file_name_suffix=cfg.datasets.output.suffix,
                                        header=cfg.datasets.output.header
                                        )
        #| "Show" >> beam.Map(print)
    )

    # run pipeline
    pipeline.run()


@hydra.main(version_base=None, config_path="config")
def main(cfg : DictConfig): 
    print(OmegaConf.to_yaml(cfg))
    if cfg.jobs.id == "pipeline_udemy":
        pipeline_udemy(cfg.jobs)
    elif cfg.jobs.id == "pipeline_alura":
        pipeline_alura(cfg.jobs)
    



if __name__ == "__main__":
    main()
