Copy pipeline/logstash*.conf_template to pipeline/logstash*.conf

Update pipeline/logstash*.conf to point to your elasticsearch server

Using Kibana or vanilla rest API - create index and mapping:

```
PUT _template/pos-cancellations
{
  "template": "pos-cancellations*",
  "mappings": {
    "logs" : {
        "dynamic": "strict",
        "properties" : {
            "@timestamp":    { "type": "date" },
            "transaction_id": { "type": "long" },
	    "invoice_date":   { "type": "date" },
            "prob_cancelled": { "type": "double" }
        }
    }
  }
}
```

Build docker image with logstash

```
docker build . -t openretail-logstash-pos-cancellation-probability
```

Run docker image locally

```
docker run --rm -it --env KAFKA_USERNAME=changeme --env KAFKA_PASSWORD=changeme openretail-logstash-pos-cancellation-probability:latest
```

Now push to Bluemix Container repository

```
bx login -a https://api.eu-de.bluemix.net --apikey=@../../config/apiKey.json
bx target -o chris.snow@uk.ibm.com -s retail_demo
docker build . -t  registry.eu-de.bluemix.net/openretail/openretail-logstash-pos-cancellation-probability:latest
docker push registry.eu-de.bluemix.net/openretail/openretail-logstash-pos-cancellation-probability:latest
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
