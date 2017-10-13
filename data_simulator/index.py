from flask import Flask
import threading
import os
import fcntl
import kafka_send
import time 
import random

PORT = int(os.getenv("PORT"))
CF_INSTANCE_INDEX = os.getenv('CF_INSTANCE_INDEX')

app = Flask(__name__)

def run_job():
    kafka_send.load_records(int(CF_INSTANCE_INDEX))

thread = threading.Thread(target=run_job)
thread.start()

@app.route('/')
def home():
    return('Coming soon...')

@app.route('/simulate_risky_transaction')
def simulate_risky_transaction():
    try:
        producer = kafka_send.get_producer()

        tx_time = int(round(time.time() * 1000))
        tx_id = random.randint(100000000000000,999999999999999)

        data='{"TransactionID":"' + str(tx_id) + '","InvoiceNo":5488202,"StockCode":"M","Description":"Manual","Quantity":1,"InvoiceDate":' + str(tx_time) + ',"UnitPrice":2053.07,"CustomerID":12744,"Country":"Singapore","LineNo":1}'

        tx_topic = os.getenv('TRANSACTIONS_TOPIC')

        producer.send(tx_topic, key='', value=data.encode('utf-8'))
        producer.flush()
        producer.close()
    
        return('Simulated transaction: ' + data)

    except Exception as e:
        print(str(e))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
