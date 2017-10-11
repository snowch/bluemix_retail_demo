Copy pipeline/logstash.conf_template to pipeline/logstash.conf

Update pipeline/logstash.conf to point to your elasticsearch server

Using Kibana or vanilla rest API - create index and mapping:

```
DELETE pos_transactions
PUT pos_transactions
PUT pos_transactions/logs/_mapping
{
   "logs" : {
        "dynamic": "strict",
        "properties" : {
            "Description": {
                "type": "text"
                    },
            "InvoiceNo": {
                "type": "long"
                    },
            "CustomerID": {
                "type": "long"
                    },
            "TransactionID": {
                "type": "long"
                    },
            "Quantity": {
                "type": "long"
                    },
            "UnitPrice": {
                "type": "double"
                    },
            "InvoiceTime": {
                "type": "text"
                    },
            "StoreID": {
                "type": "long"
                    },
            "Country": {
                "type": "text"
                    },
            "InvoiceDate": {
                "type": "date",
                "format": "dateOptionalTime"
                    },
            "StockCode": {
                "type": "text"
                    },
            "LineNo": {
                "type": "long"
                    }
        }
    }
```

Build docker image with logstash

```
docker build . -t openretail-logstash
```

Run docker image locally

```
docker run --rm -it --env KAFKA_USERNAME=changeme --env KAFKA_PASSWORD=changeme openretail-logstash:latest
```
