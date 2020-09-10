package com.ubirch.util.elasticsearch.config

object EsHighLevelConfigKeys {

  private val prefix = "esHighLevelClient"

  /*
   * connection
   *****************************************************************************/

  private val connectionPrefix = s"$prefix.connection"

  val HOST = s"$connectionPrefix.host"
  val PORT = s"$connectionPrefix.port"
  val SCHEME = s"$connectionPrefix.scheme"
  val USER = s"$connectionPrefix.user"
  val PASSWORD = s"$connectionPrefix.password"
  val MAX_RETRIES = s"$connectionPrefix.maxRetries"
  val RETRY_DELAY_FACTOR = s"$connectionPrefix.retry_delay_factor"
  val CONNECTION_TIMEOUT = s"$connectionPrefix.connectionTimeout"
  val SOCKET_TIMEOUT = s"$connectionPrefix.socketTimeout"
  val CONNECTION_REQUEST_TIMEOUT = s"$connectionPrefix.connectionRequestTimeout"


  /*
   * bulk
   *****************************************************************************/

  private val bulkPrefix = s"$prefix.bulk"

  val BULK_ACTIONS: String = s"$bulkPrefix.bulkActions"

  val BULK_SIZE: String = s"$bulkPrefix.bulkSize"

  val FLUSH_INTERVAL: String = s"$bulkPrefix.flushInterval"

  val CONCURRENT_REQUESTS: String = s"$bulkPrefix.concurrentRequests"

}
