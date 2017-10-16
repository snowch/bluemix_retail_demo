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

print('Starting index.py')


opts = {}

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

opts['topic-transactions'] = os.getenv('TRANSACTIONS_TOPIC')
opts['topic-metrics'] = os.getenv('METRICS_TOPIC')

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

def get_offset_s(store_num):
    offset = 0
    if store_num != 0:
        offset = randint(-60, 60)
    return offset

def write_lines_processed(lines_processed):
    print(str(lines_processed), end=" ", flush=True)
    # TODO write stats to a kafka topic
    #with open('processed.log', 'w') as log_f:
    #    log_f.write(str(lines_processed))

def write_tps(tps):
    #print(str(tps))
    pass
    # TODO write stats to a kafka topic
    #with open('tps.log', 'w') as log_f:
    #    log_f.write(str(tps))

def kafka_send_callback(args):
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
        if rundate_yyyymmdd in runon:
            print('runon{} contains {} - waiting until tomorrow before processing again.'.format(runon, rundate_yyyymmdd))
            time.sleep(600) # sleep 10 minutes
            continue
        else:
            print("runon{} doesn't contains {} - processing file.".format(runon, rundate_yyyymmdd))

        print('Starting new loop')

        # start with a fresh producer
        producer = get_producer()

        with gzip.open('OnlineRetail.json.gz', 'rt') as tx_f:

            lines_processed = 0
            write_lines_processed(lines_processed)
            write_tps(0)

            start_time = datetime.datetime.today().timestamp()

            for line in tx_f:

                # introduce some variability to the transactions
                if randint(0, 9) <= 3:
                    continue

                # load the record - use an OrderedDict to keep the fields in order
                j = json.loads(line, object_pairs_hook=OrderedDict)
                tx_dt = datetime.datetime.fromtimestamp(int(j['InvoiceDate'])/1000)
                tx_dt = tx_dt.replace(year=rundate.year, month=rundate.month, day=rundate.day)
                # add some randomness to the invoicedate
                tx_dt = tx_dt + datetime.timedelta(seconds=get_offset_s(store_num))
              
                tx_time = tx_dt.strftime('%H:%M:%S')
                j['InvoiceTime'] = tx_time
                j['InvoiceDate'] = int(tx_dt.strftime('%s')) * 1000

                j['StoreID'] = int(store_num)

                # TODO - use 3 digits for store, 4 for line num
                j['TransactionID'] = str(j['InvoiceNo']) + str(j['LineNo']) + str(store_num) + tx_dt.strftime('%y%m%d')

                if lines_processed == 0:
                    print('Processing first record', json.dumps(j))

                approx_wait_cycles = 0
                while datetime.datetime.now() < tx_dt:
                    # it isn't time to send our transaction yet
                    time.sleep(0.25) # yield CPU
                    approx_wait_cycles += 1
                    if approx_wait_cycles > 1000:
                        print("Waited a few cycles to process {} {}".format(j['TransactionID'], j['InvoiceDate']), flush=True)
                        approx_wait_cycles = 0 # reset the counter so we only ouput every X cycles

                # todo call back for printing error
                producer.send(opts['topic-transactions'], key=j['TransactionID'], value=json.dumps(j).encode('utf-8')).add_both(kafka_send_callback)
                lines_processed += 1

                # print status every 10000 records processed
                if lines_processed < 10 or lines_processed % 10000 == 0:
                    write_lines_processed(lines_processed)

                if lines_processed % 1000 == 0:
                    time_diff = datetime.datetime.today().timestamp() - start_time
                    write_tps(1000 / time_diff)
                    start_time = datetime.datetime.today().timestamp()

            producer.flush()
            producer.close()
            print('Finished processing records. Flushed and Closed producer ...')

            runon.append(rundate_yyyymmdd)
            print('Added {} to runon{}'.format(rundate_yyyymmdd, runon))

if __name__ == "__main__":

    store_num = os.getenv('CF_INSTANCE_INDEX')
    load_records(store_num)

