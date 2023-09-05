from confluent_kafka import Producer
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

if __name__ == "__name__":
    parse = ArgumentParser()
    parse.add_argument('config_file', type=FileType('r'))
    args = parse.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print('Error: message failed Delivery: {}'.format(err))
        else:
            print("produced event to topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    
    topic = "testtopic"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0
    for _ in range(10):

        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()