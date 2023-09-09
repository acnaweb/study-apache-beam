import apache_beam as beam
from google.cloud import bigtable


def list_to_dict(record, columns):
    return dict(zip(columns, record))


def load_file(pipeline, file):
    return pipeline | "Load file " >> beam.io.ReadFromText(file) 


def save_file(data, file):
    return data | "Save file " >> beam.io.WriteToText(file)


def save_bigtable(data, ):
    transformed_data = data | 'Convert to DirectRow' >> beam.Map(lambda row: bigtable.row.DirectRow(row))
    transformed_data | 'Write to Bigtable' >> beam.io.WriteToBigTable( project_id='my-project', instance_id='my-bigtable-instance', table_id='my-table')

    

def save_bigquery(data, table_name, schema, gcs_temp_location):
    return data | "Write to Bigquery" >> beam.io.WriteToBigQuery(
                table_name,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=gcs_temp_location
            )