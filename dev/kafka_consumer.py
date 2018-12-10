import argparse
import logging
from kafka import KafkaConsumer

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Kafka producer')
    parser.add_argument('--host', type=str, default="localhost",
                        help='Kafka host, default: localhost')
    parser.add_argument("--port", type=str, default="9092",
                        help="Kafka port, default: 9092")
    parser.add_argument("--topic", type=str, required=True,
                        help="Kafka topic. required")

    args = parser.parse_args()

    server = f'{args.host}:{args.port}'
    logging.info(f"Connecting to kafka: {server}")
    consumer = KafkaConsumer(args.topic)
    for msg in consumer:
        logging.info(msg)
