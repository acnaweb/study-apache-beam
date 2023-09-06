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
    subscription = cfg.pubsub.subscription

    def show_msg(message):
        print(message)
        message.ack()

    subscriber = pubsub_v1.SubscriberClient()
    subscriber.subscribe(subscription, callback=show_msg)

    while True:
        time.sleep(1)
    
if __name__ == "__main__":
    main()
