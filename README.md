# Twitter Streaming

Sample application that produces data from twitter to kafka, and uploads it to Google BigQuery using spark

## Setup

For run this sample I used a single kafka broker, to see how start kafka using docker see at kafka-docker

## Usage

Start producer

    python src/producer/twitter/producer.py --kafka_broker localhost:9092 --twitter_credentials credentials/twitter.yaml TWEETS.JSON keyword1,keyword2

Start consumer

    spark-submit --master local[2] --jars /opt/spark-2.4.6/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar load.py --kafka_broker localhost:2181 --gcp_credentials credentials/gcp.json TWEETS.JSON project_id.dataset.table