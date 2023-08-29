import apache_beam as beam
from apache_beam.io.textio import WriteToText

def prepare_csv(element, separator=";"):
    
    return f"{separator}".join(element)


def save(dataset, output_setup):
    tag = "[save]/"
    result = (
        (dataset)
        | f"{tag}Prepare CSV" >> beam.Map(prepare_csv, output_setup.separator)
        | f"{tag}Write to Text" >> 
            WriteToText(file_path_prefix=output_setup.path, 
                        file_name_suffix=output_setup.suffix,
                        header=output_setup.header)
        # | f"{tag}Print" >> beam.Map(print)        
    )
    return result