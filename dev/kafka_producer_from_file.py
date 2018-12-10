import argparse
import logging
from kafka import KafkaProducer


def read_file(file_path: str):
    with open(file_path, "r") as file:
        lines = file.readlines()
    return [line.strip() for line in lines]


def send_messages(producer: KafkaProducer, messages: list, topic: str, timeout=10):
    """
    Send messages to kafka
    :param producer: kafka producer
    :param messages: list of messages
    :param topic: topic name
    :param timeout: timeout between sends in seconds
    :return:
    """
    for message in messages:
        future = producer.send(topic, str.encode(message))
        future.get(timeout=timeout)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Kafka producer')
    parser.add_argument('--host', type=str, default="localhost",
                        help='Kafka host, default: localhost')
    parser.add_argument("--port", type=str, default="9092",
                        help="Kafka port, default: 9092")
    parser.add_argument("--file", type=str, required=True,
                        help="Path to file. required")
    parser.add_argument("--topic", type=str, required=True,
                        help="Kafka topic. required")

    args = parser.parse_args()

    lines = read_file(args.file)
    server = f'{args.host}:{args.port}'
    logging.info(f"Connecting to kafka: {server}")
    kafka_producer = KafkaProducer(bootstrap_servers=server)
    topic = args.topic
    logging.info(f"Writing to topic: {topic}")
    send_messages(kafka_producer, lines, topic)
    logging.info(f"Stop")



