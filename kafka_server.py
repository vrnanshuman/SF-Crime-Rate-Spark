import producer_server

def run_kafka_server(config):
    input_file = "police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=config['topic'],
        bootstrap_servers=config['brokers'],
        client_id=config['client_id']
    )
    return producer


def feed(config):
    producer = run_kafka_server(config)
    producer.generate_data()


if __name__ == "__main__":
    config = {
        "brokers" : "localhost:9092",
        "topic" : "com.udacity.sf.crime.rate",
        "client_id" : "1"
    }
    feed(config)
