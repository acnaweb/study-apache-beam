from omegaconf import DictConfig
from file_to_file import FileToFile
from file_to_bigquery import FileToBigQuery


class DataIngestion:

    def __init__(self, cfg : DictConfig) -> None:
        self.cfg = cfg
        self.job_type = self.cfg.job.type.upper()
        self.job_name = self.cfg.job.name
        self.data_ingestion = None

        if self.job_type == "FILE_TO_FILE":
            self.data_ingestion = FileToFile(self.cfg)
        elif self.job_type == "FILE_TO_BIGQUERY":
            self.data_ingestion = FileToBigQuery(self.cfg)
        else:
            raise Exception(f"Invalid job_type")

    def run(self, callback) -> None:
        print(f"Job: {self.job_name}")
        self.data_ingestion.run(callback)
