#!/usr/local/bin/python3

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.producer.future import RecordMetadata

import sys
import ssl
import json
import time
import os
import gzip
from random import randint

import datetime
import time

from collections import OrderedDict

#import logging
#import sys
#logger = logging.getLogger('kafka')
#logger.addHandler(logging.StreamHandler(sys.stdout))
#logger.setLevel(logging.INFO)

print('Starting kafka_send.py')

opts = {}

opts['topic-transactions'] = os.getenv('TRANSACTIONS_TOPIC')
opts['topic-metrics'] = os.getenv('METRICS_TOPIC')

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

def get_producer():
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

def get_now(store_num, rundate):
    offset = 0
    if store_num != 0:
        offset = randint(-60, 60)

    now = datetime.datetime.now()
    now = now + datetime.timedelta(seconds=offset)
    now = now.replace(year=rundate.year, month=rundate.month, day=rundate.day)
    return now

def write_lines_processed(lines_processed):
    #print(str(lines_processed))
    pass
    # TODO write stats to a kafka topic
    #with open('processed.log', 'w') as log_f:
    #    log_f.write(str(lines_processed))

def write_tps(tps):
    #print(str(tps))
    pass
    # TODO write stats to a kafka topic
    #with open('tps.log', 'w') as log_f:
    #    log_f.write(str(tps))

def my_custom_log_method(args):
    if type(args) is RecordMetadata:
        pass
        # Ignore success as there will be so many - instead we should track failures?
        # print('.', end="")    
    elif type(args) is KafkaTimeoutError:
        print('!', end="")
        # KafkaTimeoutError: ('Batch containing %s record(s) expired due to timeout while requesting metadata from brokers for %s', 49, TopicPartition(topic='transactions_load', partition=2))
    else:
        print(args)

def load_records(store_num):

    runon = [] # store the dates yyyymmdd that the load has already run

    # this loop will cause the load to run once every day
    while True:

        # if we've already run today, keep skipping until tomorrow
        rundate = datetime.datetime.now()
        rundate_yyyymmdd = rundate.strftime("%Y%m%d")
        if rundate in runon:
            print('runon{} contains {} - skipping.'.format(runon, rundate_yyyymmdd))
            time.sleep(600) # sleep 10 minutes
            continue

        print('Starting new loop')

        # start with a fresh producer
        producer = get_producer()

        with gzip.open('OnlineRetail.json.gz', 'rt') as tx_f:

            lines_processed = 0
            write_lines_processed(lines_processed)
            write_tps(0)

            start_time = datetime.datetime.today().timestamp()

            for line in tx_f:

                # load the record - use an OrderedDict to keep the records in order
                j = json.loads(line, object_pairs_hook=OrderedDict)
                tx_time = datetime.datetime.fromtimestamp(int(j['InvoiceDate'])/1000)
                tx_time = tx_time.replace(year=rundate.year, month=rundate.month, day=rundate.day)
                j['InvoiceDate']=int(tx_time.strftime('%s')) * 1000

                j['StoreID'] = int(store_num)

                # TODO - use 3 digits for store, 4 for line num
                j['TransactionID'] = str(j['InvoiceNo']) + str(j['LineNo']) + str(store_num)

                if lines_processed == 0:
                    print('Processing first record', json.dumps(j))

                if j['InvoiceTime'] != '00:00:00':
                    while tx_time > get_now(store_num, rundate):
                        time.sleep(0.25) # yield CPU

                # todo call back for printing error
                producer.send(opts['topic-transactions'], key=j['TransactionID'], value=json.dumps(j).encode('utf-8')).add_both(my_custom_log_method)
                lines_processed += 1

                # print status every 100 records processed
                if lines_processed < 100 or lines_processed > 2670000 or lines_processed % 100 == 0:
                    write_lines_processed(lines_processed)

                if lines_processed % 1000 == 0:
                    time_diff = datetime.datetime.today().timestamp() - start_time
                    write_tps(1000 / time_diff)
                    start_time = datetime.datetime.today().timestamp()

            producer.flush()
            producer.close()
            print('Finished processing records. Flushed and Closed producer ...')

            #rundate = time.strftime("%Y%m%d")
            runon.append(rundate_yyyymmdd)
            print('Added {} to runon{}'.format(rundate_yyyymmdd, runon))
