import apache_beam as beam
from apache_beam.io import ReadFromText


def add_key_uf_ano_mes(element):
    data, mm , uf = element
    ano_mes = "-".join(data.split("-")[:2])
    uf_ano_mes = f"{uf}-{ano_mes}"
    
    mm = float(mm) if float(mm) >= 0 else 0.0

    return (uf_ano_mes, mm)


def round_mm(element):
    key, mm = element
    return (key, round(mm, 1))


def build_pipeline_chuvas(pipeline, dataset_setup):
    tag = "[chuvas]/"
    result = (
        pipeline 
        | f"{tag}Load dataset" >> ReadFromText(dataset_setup.path, 
                                        skip_header_lines=dataset_setup.skip_header_lines)            
        | f"{tag}Text to list" >> beam.Map(lambda line: line.split(dataset_setup.separator))
        | f"{tag}Add key uf-ano-mes" >> beam.Map(add_key_uf_ano_mes)                
        | f"{tag}Sum 'casos'" >> beam.CombinePerKey(sum)
        | f"{tag}Round results" >> beam.Map(round_mm)
        # | f"{tag}Print" >> beam.Map(print)        
    )
    return result
