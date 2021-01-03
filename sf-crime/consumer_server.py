from kafka import KafkaConsumer
from json import loads


def run_consume_server():
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        group_id="test",
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    consumer.subscribe(['udacity.police_service_calls.v1'])

    for message in consumer:
        print(message.value)


if __name__ == "__main__":
    run_consume_server()