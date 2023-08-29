import apache_beam as beam

def prepare_csv(element, separator=";"):
    
    return f"{separator}".join(element)


def save(dataset):
    tag = "[save]/"
    result = (
        (dataset)
        | f"{tag}Prepare CSV" >> beam.Map(prepare_csv)
        | f"{tag}Print" >> beam.Map(print)        
    )
    return result