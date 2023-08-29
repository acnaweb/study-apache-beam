import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from .utils import list_to_dict


def add_field_ano_mes(element):
  element["ano_mes"] = "-".join(element["data_iniSE"].split("-")[:2])
  return element


def add_key_uf(element):
  return (element["uf"], element)


def add_key_uf_ano_mes(element):
  uf, data = element
  for item in data:
      if bool(re.search(r'\d', item["casos"])) == True:
        yield (f"{uf}-{item['ano_mes']}", float(item["casos"]))
      else:
        yield (f"{uf}-{item['ano_mes']}", 0.0)    


def build_pipeline_dengue(pipeline, dataset_setup):
  tag = "[dengue]/"
  # pcollection
  result = (
      pipeline
      | f"{tag}Load dataset" >> ReadFromText(dataset_setup.path, 
                                        skip_header_lines=dataset_setup.skip_header_lines)            
      | f"{tag}Text to list" >> beam.Map(lambda line: line.split(dataset_setup.separator))
      | f"{tag}List to dict" >> beam.Map(list_to_dict, dataset_setup.columns)
      | f"{tag}Add 'ano_mes'" >> beam.Map(add_field_ano_mes)
      | f"{tag}Add key uf" >> beam.Map(add_key_uf)
      | f"{tag}Group by key uf" >> beam.GroupByKey()
      | f"{tag}Add key uf-ano-mes" >> beam.FlatMap(add_key_uf_ano_mes)
      | f"{tag}Sum 'casos'" >> beam.CombinePerKey(sum)
      # | f"{tag}Print" >> beam.Map(print)        
  )

  return result
