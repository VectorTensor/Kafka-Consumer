import os
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from dotenv import load_dotenv

load_dotenv("/vault/secrets/config")


def ensure_topic_exists(conf, topic_name):
    """Checks if the topic exists and creates it if it doesn't."""
    admin_client = AdminClient(
        {'bootstrap.servers': conf['bootstrap.servers']})

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
<<<<<<< HEAD
            print(f"Topic '{topic_name}' does not exist. Creating it...")
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
=======
            logger.info(f"Topic '{topic_name}' does not exist. Creating it...")
            new_topic = NewTopic(
                topic_name, num_partitions=1, replication_factor=1)
>>>>>>> 036c84597087bd6d7df0376b7d9b53b784855ded
            fs = admin_client.create_topics([new_topic])

            # Wait for each operation to finish.
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    print(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    print(f"Failed to create topic '{topic}': {e}")
        else:
            print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Error checking/creating topic: {e}")


def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '')
    if bootstrap_servers:
        print(f" bootstrap_servers : {bootstrap_servers}")
    else:
        print("env not found")
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': os.getenv('KAFKA_GROUP_ID', 'python-consumer-group'),
        'auto.offset.reset': 'earliest'
    }

    topic = os.getenv('KAFKA_TOPIC', 'test-topic')

    # Ensure topic exists before starting consumer
    ensure_topic_exists(conf, topic)

    consumer = Consumer(conf)

    try:
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
<<<<<<< HEAD
                    print(f"End of partition reached: {msg.topic()} [{msg.partition()}]")
=======
                    logger.info(f"End of partition reached: {
                                msg.topic()} {msg.partition()}")
>>>>>>> 036c84597087bd6d7df0376b7d9b53b784855ded
                else:
                    print(f"Error occurred: {msg.error()}")
            else:
<<<<<<< HEAD
                print(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()}")
=======
                logger.info(f"Received message: {msg.value().decode(
                    'utf-8')} from topic: {msg.topic()}")
>>>>>>> 036c84597087bd6d7df0376b7d9b53b784855ded

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
