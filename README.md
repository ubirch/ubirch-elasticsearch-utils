# ubirch-elasticsearch-utils
Elasticsearch client using the High Level Java Client


# testing

To run the tests, start the elasticsearch with help of the command 
'docker-compose up' in a terminal in the root folder. Then run the 
tests as usual.

# config

The 'retry delay factor' is the number of seconds which will become multiplied with
the number of executed retries to define the actual delay before a retry is started. 
 

```
esHighLevelClient {
  connection {
    host = ${ES_HOST}
    port = ${ES_PORT}
    scheme = ${ES_SCHEME}
    user = ${ELASTIC_IO_USER} #optional else no authentication
    password = ${ELASTIC_IO_PASSWORD} #optional else no authentication
    maxRetries = ${ES_MAX_NUMBER_OF_RETRIES}
    retry_delay_factor = ${ES_DELAY_FACTOR_IN_SECONDS}
    connectionTimeout = ${ES_CONNECTION_TIMEOUT} #optional else default value 
    socketTimeout = ${ES_SOCKET_TIMEOUT} #optional else default value
    connectionRequestTimeout = ${ES_CONNECTION_REQUEST_TIMEOUT} #optional else default value
    maxConnectionPerRoute = ${ES_MAX_CONNECTION_PER_ROUTE} #optional else default value = 10
    maxConnectionTotal = ${ES_MAX_CONNECTION_TOTAL} #optional else default value = 30
  }
  bulk {
    bulkActions = ${ES_CLIENT_BULK_ACTIONS}
    bulkSize = ${ES_CLIENT_BULK_SIZE} # bulkSize in mega bytes
    flushInterval = ${ES_CLIENT_BULK_FLUSH} # flush every x seconds
    concurrentRequests = ${ES_CLIENT_CONCURRENCY} # connection pooling: max concurrent requests
  }
}
```

