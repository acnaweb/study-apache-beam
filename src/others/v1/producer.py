import hydra
import time
from google.cloud import pubsub_v1
from omegaconf import DictConfig, OmegaConf


@hydra.main(version_base=None)
def main(cfg : DictConfig): 
    print(OmegaConf.to_yaml(cfg))

     # Has Pubsub     
    pubsub = {
        "topic": cfg.pubsub.topic,            
    }

    input_file = {
        "file": cfg.input.file,
        "separator": cfg.input.separator
    }

    publisher = pubsub_v1.PublisherClient()

    with open(input_file["file"], "rb") as file:
        for row in file:
            print(row)
            publisher.publish(pubsub["topic"], row)
            time.sleep(1)

if __name__ == "__main__":
    main()
