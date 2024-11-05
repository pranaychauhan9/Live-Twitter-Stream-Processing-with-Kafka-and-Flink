from confluent_kafka import Consumer, KafkaException
import signal

topics = "covid_tweets_1,covid_tweets_2,covid_tweets_3,covid_tweets_4,covid_tweets_5,covid_tweets_6,covid_tweets_7,covid_tweets_8,covid_tweets_9,covid_tweets_10,covid_tweets_11,covid_tweets_12,covid_tweets_13,covid_tweets_14,covid_tweets_15,covid_tweets_16,covid_tweets_17,covid_tweets_18"
group_id = "my_consumer_group"   # Replace with your desired consumer group ID

conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker information
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

def shutdown(signal, frame):
    print("Shutting down Kafka consumer")
    consumer.close()
    exit(0)

signal.signal(signal.SIGINT, shutdown)

try:
    # Subscribe to topics
    consumer.subscribe(topics.split(','))

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition {msg.topic()} [{msg.partition()}]")
            else:
                # Error
                print(f"Error: {msg.error()}")
        else:
            # Process the received message
            print(f"Received message: {msg.value().decode('utf-8')} from topic {msg.topic()} [{msg.partition()}]")

except KeyboardInterrupt:
    pass

finally:
    # Close down consumer to commit final offsets.
    consumer.close()