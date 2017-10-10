from flask import Flask
import threading
import os
import fcntl

PORT = int(os.getenv("PORT"))
CF_INSTANCE_INDEX = os.getenv('CF_INSTANCE_INDEX')

app = Flask(__name__)

def run_job():
    import kafka_send
    kafka_send.load_records(int(CF_INSTANCE_INDEX))

thread = threading.Thread(target=run_job)
thread.start()

@app.route('/')
def health_check():
    # TODO
    pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
