import hydra
import apache_beam as beam
from src.dengue import build_pipeline
from omegaconf import DictConfig
from apache_beam.options.pipeline_options import PipelineOptions


pipelineOptions = PipelineOptions(argc=None)
pipeline: beam.Pipeline = beam.Pipeline(options = pipelineOptions)


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg : DictConfig): 

    build_pipeline(pipeline, cfg.datasets.dengue)

    # run pipeline
    pipeline.run()


if __name__ == "__main__":
    main()