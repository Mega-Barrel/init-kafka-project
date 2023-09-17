
import json
from time import sleep
from datetime import datetime
from random import choice
from kafka import KafkaProducer

# kafka server
kafka_server = [ "192.168.0.118" ]

# create a kafka topic
topic = "test_topic"

# producer
producer = KafkaProducer(
    bootstrap_servers = kafka_server,
    value_serializer = lambda x: json.dumps(x).encode("utf-8"),
)

random_values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

while True:
    random_value = choice(random_values)
    data = {
        "test_data": {
            "random_value": random_value
        },
        "timestamp": str(datetime.now()),
        "value_status": "High" if random_value > 5 else "Low"
    }
    print(data)
    producer.send(topic, data)
    producer.flush()
    sleep(3)