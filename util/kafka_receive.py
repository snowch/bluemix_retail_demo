#!/usr/local/bin/python

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import ssl
import os
import json
import getopt
import sys

import logging
import sys
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

opts = {}

if len(sys.argv) != 3 or sys.argv[2] not in ('latest', 'earliest'):
        print('Usage: kafka_receive <topic-name> latest|earliest')
        sys.exit(-1)

opts['topic'] = sys.argv[1]
opts['auto.offset.reset'] = sys.argv[2]

print('Attempting to connect to topic: ' + opts['topic'])

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


sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

print ('connecting')

# always use a new groupid to get all the data
if opts['auto.offset.reset'] == 'earliest':
   import uuid;
   group_id = uuid.uuid4()
else:
   group_id = 'console-consumer-script'

consumer = KafkaConsumer(
                         bootstrap_servers = opts['brokers'],
                         sasl_plain_username = opts['username'],
                         sasl_plain_password = opts['password'],
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         api_version = (0,10),
                         auto_offset_reset = opts['auto.offset.reset'],
                         group_id = group_id)

if opts['topic'] not in consumer.topics():
    print("\n** ERROR: Topic '{}' not found - have you spelt it correctly? Available topics: {} **".format(opts['topic'], consumer.topics()))
    sys.exit(-1)

consumer.subscribe(opts['topic'])

print ('connected')

for msg in consumer:
    print(msg)

