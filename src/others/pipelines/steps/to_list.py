import apache_beam as beam
from utils import (list_to_dict)


def to_list(data, cfg):
    data_split = data | "Split" >> beam.Map(lambda record: record.split(cfg.dataset.input.separator))

    return data_split