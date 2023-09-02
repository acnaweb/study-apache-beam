import hydra
import apache_beam as beam
from dotenv import load_dotenv
from omegaconf import DictConfig, OmegaConf
import src.batch_udemy as batch_udemy


@hydra.main(version_base=None, config_path="config")
def main(cfg : DictConfig): 
    load_dotenv()

    # print(OmegaConf.to_yaml(cfg))
    if cfg.jobs.id == "pipeline_udemy":
        batch_udemy.create_template(cfg.jobs)


if __name__ == "__main__":
    main()
