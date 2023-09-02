import hydra
import time
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from omegaconf import DictConfig, OmegaConf

load_dotenv()


@hydra.main(version_base=None, config_path="../config")
def main(cfg : DictConfig): 
    print(OmegaConf.to_yaml(cfg))

    topic = cfg.pubsub.topic
    input = cfg.pubsub.inputs.voos.file

    publisher = pubsub_v1.PublisherClient()

    with open(input, "rb") as file:
        for row in file:
            print(row)
            publisher.publish(topic, row)
            time.sleep(1)

if __name__ == "__main__":
    main()
