from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from google.cloud import bigquery
import json
import argparse
import os


def sendPartition(iter, cred, table):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred
    table_id = table
    client = bigquery.Client()
    table = client.get_table(table_id)  # Make an API request.
    rows_to_insert = []
    for record in iter:
        rows_to_insert.append((record["id"], record["text"]))

    errors = client.insert_rows(table, rows_to_insert)  # Make an API request.
    print(errors)
    if errors == []:
        print("New rows have been added.")


def main():
    parser = argparse.ArgumentParser(description="Streaming data from Kafka to Google BigQuery using spark")
    parser.add_argument("kafka_topic", type=str, help="Kafka topic with tweets data")
    parser.add_argument("table", type=str, help="BigQuery project_id.dataset.table")
    parser.add_argument(
        "--kafka_broker", type=str, help="Kafka broker. Default: localhost:2181", default="localhost:2181"
    )
    parser.add_argument(
        "--gcp_credentials", type=str, help="Path to GCP service account", default="credentials/gcp.json",
    )
    parser.add_argument(
        "--consumer_group", type=str, help="Kafka consumer group", default="raw-event-streaming-consumer"
    )

    args = parser.parse_args()
    topic = args.kafka_topic
    broker = args.kafka_broker
    table = args.table
    consumer_group = args.consumer_group
    gcp_credentials = args.gcp_credentials

    print(topic)
    print(broker)
    print(table)
    print(consumer_group)
    print(gcp_credentials)

    sc = SparkContext(appName="PythonStreamingKafkaToBigquery")
    ssc = StreamingContext(sc, 10)

    kvs = KafkaUtils.createStream(ssc, broker, consumer_group, {topic: 1})
    # kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": broker})

    processed = kvs.map(lambda row: json.loads(row[1])).map(lambda data: {"id": data["id_str"], "text": data["text"]})
    processed.foreachRDD(lambda rdd: rdd.foreachPartition(lambda iter: sendPartition(iter, gcp_credentials, table)))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
