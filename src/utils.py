import apache_beam as beam


def load_file(pipeline, file):
    return pipeline | "Load file " >> beam.io.ReadFromText(file) 


def save_file(pcol, file):
    return pcol | "Save file " >> beam.io.WriteToText(file)


def list_to_dict(record, columns):
    return dict(zip(columns, record))