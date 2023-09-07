import hydra
import time
from google.cloud import pubsub_v1
from omegaconf import DictConfig, OmegaConf

@hydra.main(version_base=None)
def main(cfg : DictConfig): 
    print(OmegaConf.to_yaml(cfg))

    topic = cfg.topic
    input = cfg.inputs.voos.file
    subscription = cfg.subscription

    def show_msg(message):
        print(message)
        message.ack()

    subscriber = pubsub_v1.SubscriberClient()
    subscriber.subscribe(subscription, callback=show_msg)

    while True:
        time.sleep(1)
    
if __name__ == "__main__":
    main()
