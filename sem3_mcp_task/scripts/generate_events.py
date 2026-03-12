import json
import time
import random
from kafka import KafkaProducer
import argparse


def generate_events(topic='raw_events', num_events=100, rate=10):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    event_types = ['click', 'purchase', 'view', 'add_to_cart', 'login', 'logout']
    users = [f'user_{i}' for i in range(1, 101)]
    for i in range(num_events):
        event = {
            'event_id': f'evt_{int(time.time())}_{i}',
            'timestamp': time.time(),
            'event_type': random.choice(event_types),
            'user_id': random.choice(users),
            'value': round(random.uniform(1, 1000), 2),
            'metadata': {
                'source': 'generator',
                'version': '1.0'
            }
        }

        future = producer.send(topic, event)
        future.get(timeout=10)

        if i % 10 == 0:
            print(f"Generated {i}/{num_events} events")

        time.sleep(1.0 / rate)

    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate synthetic events to Kafka')
    parser.add_argument('--topic', default='raw_events', help='Kafka topic')
    parser.add_argument('--num', type=int, default=100, help='Number of events')
    parser.add_argument('--rate', type=int, default=10, help='Events per second')
    args = parser.parse_args()
    generate_events(args.topic, args.num, args.rate)