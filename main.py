import hydra
import apache_beam as beam
from src.dengue import build_pipeline_dengue
from src.chuvas import build_pipeline_chuvas
from src.join_chuvas_dengue import build_pipeline_join_chuvas_dengue
from src.save import save
from omegaconf import DictConfig
from apache_beam.options.pipeline_options import PipelineOptions


pipelineOptions = PipelineOptions(argc=None)
pipeline: beam.Pipeline = beam.Pipeline(options = pipelineOptions)

@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg : DictConfig): 

    dengue = build_pipeline_dengue(pipeline, cfg.datasets.dengue)
    chuvas = build_pipeline_chuvas(pipeline, cfg.datasets.chuvas)
    dataset_chuvas_dengue = build_pipeline_join_chuvas_dengue(chuvas, dengue)
    save(dataset_chuvas_dengue, cfg.datasets.output)

    # run pipeline
    pipeline.run()


if __name__ == "__main__":
    main()
