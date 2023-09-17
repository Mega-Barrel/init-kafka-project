import json
from kafka import ( KafkaConsumer )

# kafka server
kafka_server = [ "192.168.0.118" ]

# create a kafka topic
topic = "test_topic"

consumer = KafkaConsumer(
    bootstrap_servers = kafka_server,
    value_deserializer = json.loads,
    auto_offset_reset = "latest",
)

consumer.subscribe(topics=topic)

while True:
    data = next(consumer)
    print(data)
    print(data.value)
    print()
