This folder is the code for a Cloud Foundry project.  It simulates transactions from stores being sent into Message Hub (Kafka).

This project uses the file OpenRetail.json.gz created by the script [data/prepare.py](../data/).  When run, the code iterates through the data file line by line.  If the current time is earlier than the transaction time, the transaction is sent to Message Hub.

Multiple instances of this project can be deployed onto Cloud Foundry.  Each instance represents data from another store.  This approach allows us to easily load test by scaling up the data load through increasing the number of Cloud Foundry instances running the code.

This cloud foundry app requires your message hub instance to be connected to it. After pushing with `cf push`, navigate to the Bkuemix console and the cloud foundry app.  Click on connections and add your Message Hub instance.
