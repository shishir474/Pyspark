"""
Kafka Producer — sends random sentences to the 'word-count-input' topic.

Install dependency:
    pip install kafka-python

Run AFTER docker-compose is up:
    python producer.py
"""

import time
import random
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "word-count-input"

SENTENCES = [
    "the quick brown fox jumps over the lazy dog",
    "apache spark is a unified analytics engine",
    "structured streaming is built on top of spark sql",
    "kafka is a distributed event streaming platform",
    "word count is the hello world of big data",
    "pyspark makes spark accessible from python",
    "streaming data pipelines process events in real time",
    "the cat sat on the mat",
    "to be or not to be that is the question",
    "all that glitters is not gold",
]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    print(f"Producing messages to topic '{TOPIC}'. Press Ctrl+C to stop.\n")

    try:
        while True:
            sentence = random.choice(SENTENCES)
            producer.send(TOPIC, value=sentence)
            print(f"Sent: {sentence}")
            time.sleep(1)  # one message per second
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
