- Copy replication.yaml_template to replication.yaml
- Update replication.yaml with your kafka credentials
- Copy pipeline/logstash*.conf_template to pipeline/logstash*.conf
- Update pipeline/logstash*.conf to point to your elasticsearch server

Using Kibana or vanilla rest API - create index and mapping:

```
PUT _template/pos-transactions
{
  "template": "pos-transactions*",
  "mappings": {
    "logs" : {
        "dynamic": "strict",
        "properties" : {
            "@timestamp":    { "type": "text" },
            "Description":   { "type": "text" },
            "InvoiceNo":     { "type": "long" },
            "CustomerID":    { "type": "long" },
            "TransactionID": { "type": "long" },
            "Quantity":      { "type": "long" },
            "UnitPrice":     { "type": "double" },
            "InvoiceTime":   { "type": "text" },
            "StoreID":       { "type": "long" },
            "Country":       { "type": "text" },
            "InvoiceDate":   { "type": "date" },
            "StockCode":     { "type": "text" },
            "LineNo":        { "type": "long" }
        }
    }
  }
}
```

Build docker image with logstash

```
docker build . -t openretail-logstash-pos-transactions
```

Run docker image locally

```
docker run --rm -it --env KAFKA_USERNAME=changeme --env KAFKA_PASSWORD=changeme openretail-logstash-pos-transactions:latest
```

Now push to Bluemix Container repository

```
bx login -a https://api.eu-de.bluemix.net --apikey=@../../config/apiKey.json
bx target -o chris.snow@uk.ibm.com -s retail_demo
docker build . -t  registry.eu-de.bluemix.net/openretail/openretail-logstash-pos-transactions:latest
docker push registry.eu-de.bluemix.net/openretail/openretail-logstash-pos-transactions:latest
bx cr image-list
```

Now run in Kubernetes

```
bx cs init
bx cs cluster-config my_kubernetes
```

Execute the line `export KUBECONFIG=...` returned from the previous command

```
kubectl create -f replication.yaml
kubectl proxy
```

Navigate to URL returned by last command ^
