import hydra
from dotenv import load_dotenv
import apache_beam as beam
from omegaconf import DictConfig, OmegaConf
import src.batch_beam as batch_beam
import src.batch_dataflow.job as job

load_dotenv()

@hydra.main(version_base=None, config_path="config")
def main(cfg : DictConfig): 

    # print(OmegaConf.to_yaml(cfg))
    if cfg.batch:
        if cfg.batch.id == "batch_beam":
            batch_beam.run(cfg.batch)
        elif cfg.batch.id == "batch_dataflow":
            job.run(cfg.batch)


if __name__ == "__main__":
    main()
