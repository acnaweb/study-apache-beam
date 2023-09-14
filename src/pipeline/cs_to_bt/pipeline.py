import hydra
import apache_beam as beam
from google.cloud.bigtable import row
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from omegaconf import DictConfig, OmegaConf



class ConvertToJson(beam.DoFn):

    def process(self, element):
        import json
        return json.dumps(element)
        


class DhuoFlowUtils:


    @staticmethod
    def schema_as_line(columns) -> str:     
            return ",".join(columns)


    @staticmethod
    def list_to_dict(record, columns):
        return dict(zip(columns, record))


    @staticmethod
    def to_dict(data, columns):
        return data | "List To Dict" >> beam.Map(DhuoFlowUtils.list_to_dict, columns)


    @staticmethod
    def to_list(data, separator):
        data_split = data | "Split" >> beam.Map(lambda record: record.split(separator))
        return data_split


    @staticmethod
    def load_file(data, file):
        return data | "Load file " >> beam.io.ReadFromText(file) 


    @staticmethod
    def save_file(data, file):
        return data | "Save file " >> beam.io.WriteToText(file)


    @staticmethod
    def save_bigquery(data, table_name, schema, gcs_temp_location):
        return data | "Write to Bigquery" >> beam.io.WriteToBigQuery(
                    table_name,
                    schema=schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=gcs_temp_location
                )

    @staticmethod
    def save_bigtable(data, project_id, instance_id, table_id):        
        return data | 'Write to Bigtable' >> WriteToBigTable( 
                    project_id=project_id, 
                    instance_id=instance_id, 
                    table_id=table_id)


    @staticmethod
    def build_dataflow_options(cfg : DictConfig):
        return  {
            "project": cfg.gcp.project,
            "runner": "DataflowRunner",
            "region": cfg.gcp.dataflow.region,
            "staging_location": cfg.gcp.dataflow.staging_location,
            "temp_location": cfg.gcp.temp_location,
            "template_location": f"{cfg.gcp.dataflow.template_location}/{cfg.job.name}",
            "save_main_session": cfg.gcp.dataflow.save_main_session,
            "subnetwork": cfg.gcp.dataflow.subnetwork,
            "streaming": cfg.gcp.dataflow.streaming,
            "max_num_workers": cfg.gcp.dataflow.max_num_workers,
            "setup_file": "./setup.py"
        }


class CreateRowFn(beam.DoFn):
 
    def process(self, key):
        
        from google.cloud.bigtable import row
        import datetime

        

        direct_row = row.DirectRow(row_key=key)
        direct_row.set_cell(
            "stats_summary",
            b"os_build",
            b"android",
            datetime.datetime.now())
        
        # direct_row.commit()

class CreateHbaseRow(beam.DoFn): 
    
    
    def __init__(self, project_id, instance_id, table_id):
      self.project_id = project_id
      self.instance_id = instance_id
      self.table_id = table_id
    

    def start_bundle(self):
        import logging
        from google.cloud import bigtable
        from google.cloud.bigtable import column_family
        try:
            self.client = bigtable.Client(project=self.project_id, admin=True) 
            self.instance = self.client.instance(self.instance_id) 
            self.table = self.instance.table(self.table_id)

            max_versions_rule = column_family.MaxVersionsGCRule(2)
            
            # column family 
            column_family_fullname = "fullname"
            column_family_location = "location"
            column_families = {column_family_fullname: max_versions_rule, 
                               column_family_location: max_versions_rule}

            if not self.table.exists():
                self.table.create(column_families=column_families)
            else:
                logging.info("Table {} already exists.".format(self.table_id))
        except:
            logging.error("Failed to start bundle")
            raise       


    def process(self, element):
        import logging
        from datetime import datetime  

        # column family 
        column_family_fullname = "fullname"
        column_family_location = "location"

        print(element.items())
        
        try:
            table = self.table
            rows = []  
            row = table.row(row_key=str(element['id']).encode())  
            for colname, val in element.items():                
                logging.info("{} {} +++".format(colname, val))

                if val: 
                    if isinstance(val, str):
                        logging.info("Creating row for: {} {} ***".format(colname, val))
                        valenc = val.encode("utf-8")
                    else:
                        valenc = bytes(val)    

                    if colname in ["first_name", "last_name"]:
                        row.set_cell(column_family_fullname,colname.encode("utf-8"),valenc, datetime.now())
                    elif colname in ["address", "city"]:
                        row.set_cell(column_family_location,colname.encode("utf-8"),valenc, datetime.now())

            rows.append(row)    
            table.mutate_rows(rows)   
        except:   
            logging.error("Failed with input: ", str(element))
            raise
    

class DhuoFlow:

    def __init__(self, cfg : DictConfig) -> None:
        self.cfg = cfg
        self.validate()


    def validate(self) -> bool:
        if not self.cfg.dataset.input.file:
            raise Exception("cfg.dataset.input.file not found")
        elif not self.cfg.job.name:
            raise Exception("cfg.job.name not found")
        # elif not self.cfg.dataset.output.table:
        #     raise Exception("cfg.dataset.output.table not found")
        elif not self.cfg.dataset.output.schema:    
            raise Exception("cfg.dataset.output.schema not found")
        elif not self.cfg.gcp.project:
            raise Exception("cfg.gcp.project not found")
        elif not self.cfg.gcp.temp_location:
            raise Exception("cfg.gcp.temp_location not found")     

        self.gcp_project =  self.cfg.gcp.project
        self.gcp_temp_location =  self.cfg.gcp.temp_location
        self.input_file = self.cfg.dataset.input.file
        self.input_separator = self.cfg.dataset.input.separator
        # self.table_name = f"{self.gcp_project}:{self.cfg.dataset.output.table}"
        # self.table_schema = DhuoFlowUtils.schema_as_line(self.cfg.dataset.output.schema)
        self.output_columns = self.cfg.dataset.output.columns

        self.bigtable = {
            "instance": self.cfg.dataset.output.bigtable.instance,
            "table": self.cfg.dataset.output.bigtable.table
        }

    def run(self) ->  None:
        # check if use Dataflow
        if self.cfg.use_dataflow:
            pipelineOptions = PipelineOptions.from_dictionary(DhuoFlowUtils.build_dataflow_options(self.cfg))
        else:
            pipelineOptions = PipelineOptions(argc=None)    
            
        with beam.Pipeline(options = pipelineOptions) as p:   
            # data = DhuoFlowUtils.load_file(p, self.input_file)            
            # data_as_list = DhuoFlowUtils.to_list(data, self.input_separator)
            # data_as_dict = DhuoFlowUtils.to_dict(data_as_list, self.output_columns)
            # data_as_dict | "Save Bigtable" >> beam.ParDo(CreateHbaseRow(self.gcp_project,
            #                                                             self.bigtable["instance"],
            #                                                             self.bigtable["table"]))
            data = (p 
                    | beam.Create(
                        [
                        "phone#4c410523#20190501",
                        "phone#4c410523#20190502"])
                    # | beam.ParDo(CreateRowFn(self.gcp_project,
                    #                     self.bigtable["instance"],
                    #                     self.bigtable["table"]))
                    | WriteToBigTable( 
                                project_id=self.gcp_project, 
                                instance_id="acnaweb-bigtable-instance", 
                                table_id="pessoas")
            )

            
@hydra.main(version_base=None, config_name="config", config_path=".")
def main(cfg: DictConfig):

    print(OmegaConf.to_yaml(cfg))

    dhuoflow = DhuoFlow(cfg)
    dhuoflow.run()


if __name__ == "__main__":
    main()

    