import tweepy
from kafka import KafkaProducer
import json
import argparse
import yaml
import log

logger = log.get_logger(__name__)


class StreamTweetsToKafka(tweepy.StreamListener):
    def __init__(self, broker, topic):
        super(StreamTweetsToKafka, self).__init__()
        self.topic = topic
        self.broker = broker
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            acks=1,
            key_serializer=str.encode,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def on_status(self, status):
        produce_event(self.producer, self.topic, status._json)


def produce_event(producer: KafkaProducer, topic: str, event: dict):
    event_id = event.get("id_str")
    logger.info("Publishing to %s with id %s", topic, event_id)
    producer.send(topic, value=event, key=event_id)


def parse_yaml_twitter_credentials(credentials_path):
    with open(credentials_path, "r") as f:
        credentials = yaml.safe_load(f.read())
    twitter = credentials["twitter"]
    return (
        twitter["consumer_key"],
        twitter["consumer_secret"],
        twitter["access_token"],
        twitter["access_token_secret"],
    )


def main():
    parser = argparse.ArgumentParser(
        description="Watch for a list of keywords from twitter and produces to a kafka topic"
    )
    parser.add_argument("kafka_topic", type=str, help="Kafka topic to publish raw tweets")
    parser.add_argument(
        "--kafka_broker", type=str, help="Kafka broker. Default: localhost:9092", default="localhost:9092"
    )
    parser.add_argument(
        "--twitter_credentials",
        type=str,
        help="Path to twitter api credentials in yaml format",
        default="credentials/twitter.yaml",
    )
    parser.add_argument("keywords", type=str, help="Comma-separated keywords to monitor in twitter")

    args = parser.parse_args()
    topic = args.kafka_topic
    broker = args.kafka_broker

    keywords = args.keywords.split(",")
    logger.info("Keywords: %s " % (keywords,))

    consumer_key, consumer_secret, access_token, access_token_secret = parse_yaml_twitter_credentials(
        args.twitter_credentials
    )
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    myStreamListener = StreamTweetsToKafka(broker=broker, topic=topic)
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(track=keywords)


if __name__ == "__main__":
    main()

