import logging
import os
from confluent_kafka import Consumer, KafkaError, AdminClient
from confluent_kafka.admin import NewTopic
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("KafkaConsumer")

load_dotenv("/vault/secrets/config")

def ensure_topic_exists(conf, topic_name):
    """Checks if the topic exists and creates it if it doesn't."""
    admin_client = AdminClient({'bootstrap.servers': conf['bootstrap.servers']})
    
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            logger.info(f"Topic '{topic_name}' does not exist. Creating it...")
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            
            # Wait for each operation to finish.
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Error checking/creating topic: {e}")

def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
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
        logger.info(f"Subscribed to topic: {topic}")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached: {msg.topic()} [{msg.partition()}]")
                else:
                    logger.error(f"Error occurred: {msg.error()}")
            else:
                logger.info(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
