import apache_beam as beam
from utils import (list_to_dict)


def pipeline(data, cfg):    
    data_split = data | "Split" >> beam.Map(lambda record: record.split(cfg.dataset.input.separator))
    data_dict = data_split | "List To Dict" >> beam.Map(list_to_dict, cfg.dataset.output.columns)

    return data_dict