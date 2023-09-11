import apache_beam as beam
from utils import (list_to_dict)


def pipeline(data, cfg):
    data1 = data | "Step1 Raveli" >> beam.Map(lambda record: record.split(cfg.dataset.input.separator))
    data2 = data1 | "Step2 Raveli1" >> beam.Map(lambda record: record.split(cfg.dataset.input.separator))

    return data2