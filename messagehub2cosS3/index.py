#!/usr/local/bin/python3

from kafka import KafkaConsumer 
from kafka.structs import TopicPartition, OffsetAndMetadata
import sys
import ssl
import json
import os
import boto3
from signal import *
import time
import io

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumReader, DatumWriter


exiting = False

def exit(*args):
    print('Exiting - please wait')
    global exiting
    exiting = True

for sig in (SIGABRT, SIGILL, SIGINT, SIGSEGV, SIGTERM):
    signal(sig, exit)

#import logging
#import sys
#logger = logging.getLogger('kafka')
#logger.addHandler(logging.StreamHandler(sys.stdout))
#logger.setLevel(logging.INFO)

print('Starting index.py')


def get_opts():
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

        if os.environ.get('COS_CREDENTIALS'):
            cos_credentials = json.loads(os.environ.get('COS_CREDENTIALS'))
            opts['S3_ACCESS_KEY'] = cos_credentials['S3_ACCESS_KEY']
            opts['S3_SECRET_KEY'] = cos_credentials['S3_SECRET_KEY']
            opts['S3_ENDPOINT'] = 'https://' + cos_credentials['S3_PUBLIC_ENDPOINT']
        else:
            print('ERROR: COS_CREDENTIALS environment variable not set')
            sys.exit(-1)

        if os.environ.get('KAFKA_TOPIC'):
            opts['kafka_topic'] = os.getenv('KAFKA_TOPIC')
        else:
            print('ERROR: KAFKA_TOPIC environment variable not set')
            sys.exit(-1)

        if os.environ.get('KAFKA_GROUP_ID'):
            opts['kafka_group_id'] = os.getenv('KAFKA_GROUP_ID')
        else:
            print('ERROR: KAFKA_GROUP_ID environment variable not set')
            sys.exit(-1)

        return opts
    else:
        print('ERROR: no VCAP_SERVICES found in environment')
        sys.exit(-1)
    
    if opts == {}:
        print('ERROR: no messagehub bound to application')
        sys.exit(-1)

def get_consumer(opts):
    sasl_mechanism = 'PLAIN'
    security_protocol = 'SASL_SSL'
    
    # Create a new context using system defaults, disable all but TLS1.2
    context = ssl.create_default_context()
    context.options &= ssl.OP_NO_TLSv1
    context.options &= ssl.OP_NO_TLSv1_1

    consumer = KafkaConsumer(
                         bootstrap_servers = opts['brokers'],
                         sasl_plain_username = opts['username'],
                         sasl_plain_password = opts['password'],
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         api_version = (0,10),
                         enable_auto_commit = False,
                         auto_offset_reset = 'earliest',
                         group_id = opts['kafka_group_id'],
                         #value_deserializer=str.decode
                         )

    consumer.subscribe([ opts['kafka_topic'] ])
    return consumer

def get_boto3_client(opts):
    client = boto3.resource (
        's3',
        aws_access_key_id     = opts['S3_ACCESS_KEY'],
        aws_secret_access_key = opts['S3_SECRET_KEY'],
        endpoint_url          = opts['S3_ENDPOINT']
    )
    return client

def ls_cos(client):
    # quick santity check - print buckets
    for bucket in client.buckets.all():
        print('S3 bucket found: ' + bucket.name)
        for obj in bucket.objects.all():
            print("  - %s" % obj.key)

def get_avro_schema():
    avro_file_schema = """
        {"namespace": "transaction.avro",
         "type": "record",
         "name": "Transaction",
         "fields": [
             {"name": "InvoiceNo",     "type": "int"    },
             {"name": "StockCode",     "type": "string" },
             {"name": "Description",   "type": "string" },
             {"name": "Quantity",      "type": "int"    },
             {"name": "InvoiceDate",   "type": "long"   },
             {"name": "UnitPrice",     "type": "float"  },
             {"name": "CustomerID",    "type": "int"    },
             {"name": "Country",       "type": "string" },
             {"name": "LineNo",        "type": "int"    },
             {"name": "InvoiceTime",   "type": "string" },
             {"name": "StoreID",       "type": "int"    },
             {"name": "TransactionID", "type": "string" }
         ]
        }
    """
    return avro.schema.Parse(avro_file_schema)

def load_records(opts):
    s3client = get_boto3_client(opts)
    #ls_cos(s3client)

    consumer = get_consumer(opts)

    offsets = { }
    avro_file_buffer = io.BytesIO()
    writer = DataFileWriter(avro_file_buffer, DatumWriter(), get_avro_schema())
    record_count = 0
    while not exiting:
        data_dict = consumer.poll(timeout_ms=60000, max_records=100)

        
        if data_dict is None:
            print('No data found')
        else:
            for key in data_dict.keys():
                for msg in data_dict[key]:
                    #print(msg)
                    record_count += 1

                    topic     = msg.topic
                    partition = msg.partition
                    offset    = msg.offset

                    topicPartition = TopicPartition(topic, partition)
                    offsetAndMetadata = OffsetAndMetadata(offset, b'')

                    offsets[topicPartition] = offsetAndMetadata

                    j_msg = json.loads(msg.value.decode('utf-8'))

                    # some records have encoded StockCode as int - fix this up so they are all strings
                    j_msg['StockCode'] = str(j_msg['StockCode'])
                    writer.append(j_msg)

        if record_count >= 5000:
            writer.flush()
            avro_file_buffer.seek(0)
            s3obj = s3client.Object('temp-bucket', 'transactions/{}.avro'.format(int(time.time())))
            s3obj.put(Body=avro_file_buffer)
            writer.close()
            #ls_cos(s3client) # for debugging
            #print(offsets) # for debugging
            
            # If commit fails to run successfully, we could receive duplicate data in S3 because the offsets will 
            # get reprocessed. For more information, see:
            # 
            # - https://cwiki.apache.org/confluence/display/KAFKA/FAQ#FAQ-HowdoIgetexactly-oncemessagingfromKafka?
            # 
            # TODO: provide some examples for making the processing idempotent.
            consumer.commit( offsets )

            # reset buffers
            offsets = {}
            avro_file_buffer = io.BytesIO()
            writer = DataFileWriter(avro_file_buffer, DatumWriter(), get_avro_schema())
            record_count = 0

    consumer.close(autocommit=False)

if __name__ == "__main__":
    opts = get_opts()
    load_records(opts)

