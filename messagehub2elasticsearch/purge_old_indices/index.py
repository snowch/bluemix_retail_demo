import schedule
import time
import elasticsearch
import curator
import os
import sys
from curator.exceptions import NoIndices

ES1 = os.getenv('ES_URL1')
if ES1 is None: 
    print("Error: ES_URL1 environment variable not set.")    
    sys.exit(-1)

ES2 = os.getenv('ES_URL2')
if ES2 is None: 
    print("Error: ES_URL2 environment variable not set.")    
    sys.exit(-1)

def job():
    print('Starting schedule job')
    client = elasticsearch.Elasticsearch([ ES1,ES2 ], use_ssl=True)

    def purge_index(index_name, days_to_keep):
       try:
          ilo = curator.IndexList(client)
          ilo.filter_by_regex(kind='prefix', value=index_name)
          ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=days_to_keep)
          delete_indices = curator.DeleteIndices(ilo)
          delete_indices.do_action()
       except NoIndices:
          pass

    purge_index('pos-transactions-', 3)
    purge_index('pos-cancellations-', 3)

    print('Finished schedule job')

schedule.every().hour.do(job)
#schedule.every(1).minutes.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
