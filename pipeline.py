import hydra
import apache_beam as beam
from src.dengue import build_pipeline_dengue
from src.chuvas import build_pipeline_chuvas
from omegaconf import DictConfig
from apache_beam.options.pipeline_options import PipelineOptions


pipelineOptions = PipelineOptions(argc=None)
pipeline: beam.Pipeline = beam.Pipeline(options = pipelineOptions)


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg : DictConfig): 

    build_pipeline_dengue(pipeline, cfg.datasets.dengue)
    build_pipeline_chuvas(pipeline, cfg.datasets.chuvas)

    # run pipeline
    pipeline.run()


if __name__ == "__main__":
    main()
