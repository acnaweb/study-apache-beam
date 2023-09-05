import hydra
import apache_beam as beam
from dotenv import load_dotenv
from omegaconf import DictConfig, OmegaConf
import src.batch_beam as batch_beam


@hydra.main(version_base=None, config_path="config")
def main(cfg : DictConfig): 
    load_dotenv()

    # print(OmegaConf.to_yaml(cfg))
    if cfg.batch:
        if cfg.batch.id == "beam":
            batch_beam.create_template(cfg.batch)


if __name__ == "__main__":
    main()
