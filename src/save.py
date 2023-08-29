import apache_beam as beam
from apache_beam.io.textio import WriteToText

def prepare_csv(element, separator=";"):
    
    return f"{separator}".join(element)


def save(dataset):
    tag = "[save]/"
    result = (
        (dataset)
        | f"{tag}Prepare CSV" >> beam.Map(prepare_csv)
        | f"{tag}Write to Text" >> WriteToText("./data/processed/output.csv")
        # | f"{tag}Print" >> beam.Map(print)        
    )
    return result