#!/usr/local/bin/python3

import logging
import sys
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from kafka.producer.future import RecordMetadata

import sys
import ssl
import json
import time
import os
import gzip
from random import randint

from sklearn.externals import joblib
import pandas as pd
import numpy as np

print('Starting kafka_send.py')

logistic = joblib.load('logistic.pkl')

data = np.array([[1, 1, 1]])

print('Smoke testing predictor')
print(logistic.predict_proba(data))

opts = {}

opts['topic-transactions'] = os.getenv('TRANSACTIONS_TOPIC')
opts['topic-predictions'] = os.getenv('PREDICTIONS_TOPIC')

if os.environ.get('VCAP_SERVICES'):
    # Running in Bluemix
    print('Running in Bluemix mode.')
    vcap_services = json.loads(os.environ.get('VCAP_SERVICES'))
    for vcap_service in vcap_services:
        if vcap_service.startswith('messagehub'):
            messagehub_service = vcap_services[vcap_service][0]
            opts['brokers'] = ','.join(messagehub_service['credentials']['kafka_brokers_sasl'])
            opts['api_key'] = messagehub_service['credentials']['api_key']
            opts['username'] = messagehub_service['credentials']['user']
            opts['password'] = messagehub_service['credentials']['password']
            opts['rest_endpoint'] = messagehub_service['credentials']['kafka_admin_url']
else:
    print('ERROR: no VCAP_SERVICES found in environment')
    sys.exit(-1)

if opts == {}:
    print('ERROR: no messagehub bound to application')
    sys.exit(-1)

#print(opts)

sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

def get_producer(opts):
    producer = KafkaProducer(bootstrap_servers = opts['brokers'],
                         sasl_plain_username = opts['username'],
                         sasl_plain_password = opts['password'],
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         api_version = (0,10),
                         retries = 5,
                         key_serializer=str.encode,
                         #value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )
    return producer

def get_consumer(group_id, opts):
    consumer = KafkaConsumer(
                         bootstrap_servers = opts['brokers'],
                         sasl_plain_username = opts['username'],
                         sasl_plain_password = opts['password'],
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         api_version = (0,10),
                         auto_offset_reset = 'latest',
                         group_id = group_id)
    return consumer

def kafka_send_callback(args):
    if type(args) is RecordMetadata:
        # Ignore success as there will be so many - instead we should track failures?
        pass
    elif type(args) is KafkaTimeoutError:
        print('!', end="")
        # Example error output:
        #    KafkaTimeoutError: ('Batch containing %s record(s) expired due to timeout while requesting metadata from brokers for %s', 49, TopicPartition(topic='transactions_load', partition=2))
    else:
        print(args)

producer = get_producer(opts)
consumer = get_consumer(group_id = 'streaming_predictor-2', opts)

if opts['topic-transactions'] not in consumer.topics():
    print("\n** ERROR: Topic '{}' not found - have you spelt it correctly? Available topics: {} **".format(opts['topic'], consumer.topics()))
    sys.exit(-1)

consumer.subscribe([opts['topic-transactions']])

print('Subscribed to: ' + opts['topic-transactions'])

for msg in consumer:

    #print('Processing: ' + str(msg))

    j = json.loads(msg.value)

    transaction_id = j['TransactionID']
    customer_id = j['CustomerID']
    quantity = j['Quantity']
    price = j['UnitPrice']

    data = np.array([[price, quantity, customer_id]])

    predict_prob = logistic.predict_proba(data)
    #print(predict_prob)

    prob_cancelled = predict_prob[0][1]

    #if prob_cancelled > 0.5:

    prediction = {
            'transaction_id': transaction_id,
            'prob_cancelled': prob_cancelled
            }

    producer.send(opts['topic-predictions'], key=j['TransactionID'], value=json.dumps(prediction).encode('utf-8')).add_both(kafka_send_callback)


