import schedule
import time
import elasticsearch
import curator
import os

def job():
    print('Starting schedule job')
    client = elasticsearch.Elasticsearch(
                [ os.getenv('ES_URL1'), os.getenv('ES_URL2') ],
                use_ssl=True
            )
    
    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='prefix', value='pos-transactions-')
    ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=3)
    delete_indices = curator.DeleteIndices(ilo)
    delete_indices.do_action()

    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='prefix', value='pos-cancellations-')
    ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=3)
    delete_indices = curator.DeleteIndices(ilo)
    delete_indices.do_action()
    print('Finished schedule job')

schedule.every().hour.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
