import producer_server


def run_kafka_server():
    producer = producer_server.ProducerServer(
        input_file="police-department-calls-for-service.json",
        topic="udacity.police_service_calls.v1",
        bootstrap_servers="localhost:9092",
        client_id="sf-police-service-calls"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
