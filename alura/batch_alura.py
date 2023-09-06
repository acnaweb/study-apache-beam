import apache_beam as beam
from omegaconf import DictConfig
from apache_beam.options.pipeline_options import PipelineOptions
from .chuvas import build_pipeline_chuvas
from .dengue import build_pipeline_dengue
from .join_chuvas_dengue import build_pipeline_join_chuvas_dengue
from .save import save


def create_template(cfg : DictConfig):
    pipelineOptions = PipelineOptions(argc=None)

    pipeline: beam.Pipeline = beam.Pipeline(options = pipelineOptions)
    dengue = build_pipeline_dengue(pipeline, cfg.datasets.dengue)
    chuvas = build_pipeline_chuvas(pipeline, cfg.datasets.chuvas)
    dataset_chuvas_dengue = build_pipeline_join_chuvas_dengue(chuvas, dengue)
    save(dataset_chuvas_dengue, cfg.datasets.output)

    # run pipeline
    pipeline.run()
