# Twitter Streaming

Sample application that produces data from twitter to kafka, and uploads it to Google BigQuery using spark

## Setup

For run this sample I used a single kafka broker, to see how start kafka using docker see at kafka-docker

## Usage

Start producer

    python src/producer/twitter/producer.py --kafka_broker localhost:9092 --twitter_credentials credentials/twitter.yaml TWEETS.JSON keyword1,keyword2

