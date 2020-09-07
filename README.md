# ubirch-elasticsearch-utils
Elasticsearch client using the High Level Java Client


# testing

To run the tests, start the elasticsearch with help of the command 
'docker-compose up' in a terminal in the root folder. Then run the 
tests as usual.

# config

```
esHighLevelClient {
  connection {
    host = ${ES_HOST}
    port = ${ES_PORT}
    scheme = ${ES_SCHEME}
    user = ${ELASTIC_IO_USER} #optional else no authentication
    password = ${ELASTIC_IO_PASSWORD} #optional else no authentication
    connectionTimeout = ${CONNECTION_TIMEOUT} #optional else default value 
    socketTimeout = ${SOCKET_TIMEOUT} #optional else default value
    connectionRequestTimeout = ${CONNECTION_REQUEST_TIMEOUT} #optional else default value
  }
  bulk {
    bulkActions = ${ES_CLIENT_BULK_ACTIONS}
    bulkSize = ${ES_CLIENT_BULK_SIZE} # bulkSize in mega bytes
    flushInterval = ${ES_CLIENT_BULK_FLUSH} # flush every x seconds
    concurrentRequests = ${ES_CLIENT_CONCURRENCY} # connection pooling: max concurrent requests
  }
}
```

