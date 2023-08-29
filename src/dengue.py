import apache_beam as beam
from apache_beam.io import ReadFromText
from .utils import list_to_dict


def add_field_ano_mes(element):
    element["ano_mes"] = "-".join(element["data_iniSE"].split("-")[:2])
    return element

def add_key(element):
    return (element["uf"], element)


def build_pipeline(pipeline, dataset_setup):

  # pcollection
    dengue = (
        pipeline
        | "Load dataset" >> ReadFromText(dataset_setup.path, 
                                         skip_header_lines=dataset_setup.skip_header_lines)            
        | "Text to list" >> beam.Map(lambda line: line.split(dataset_setup.separator))
        | "List to dict" >> beam.Map(list_to_dict, dataset_setup.columns)
        | "Add 'ano_mes'" >> beam.Map(add_field_ano_mes)
        | "Add key" >> beam.Map(add_key)
        | "Group by key" >> beam.GroupByKey()
        | "Show results" >> beam.Map(print)
    )
