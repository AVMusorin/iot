import argparse
import logging
from kafka import KafkaProducer
import serial
from time import sleep


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

    server = "localhost:9092"
    logging.info(f"Connecting to kafka: {server}")
    kafka_producer = KafkaProducer(bootstrap_servers=server)
    topic = "sensor"
    logging.info(f"Writing to topic: {topic}")

    with serial.Serial('/dev/ttyACM0', 115200) as ser:
        while True:
            line = ser.readline().decode('ascii')
            print(line)
            spline = line.split(" ")
            if spline[0] == "spark" and spline[1] == "1":
                line = line[10:]
                print("Sended: " + line)
                send_messages(kafka_producer, [line], topic)
           
    
    logging.info(f"Stop")



