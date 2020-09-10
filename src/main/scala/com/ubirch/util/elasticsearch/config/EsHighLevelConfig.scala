package com.ubirch.util.elasticsearch.config

import com.ubirch.util.config.ConfigBase

import scala.util.Try

object EsHighLevelConfig extends ConfigBase {


  /*
   * connection
   *****************************************************************************/

  val host: String = config.getString(EsHighLevelConfigKeys.HOST)
  val port: Int = config.getInt(EsHighLevelConfigKeys.PORT)
  val scheme: String = config.getString(EsHighLevelConfigKeys.SCHEME)
  val user: String = Try(config.getString(EsHighLevelConfigKeys.USER)).getOrElse("")
  val password: String = Try(config.getString(EsHighLevelConfigKeys.PASSWORD)).getOrElse("")
  val maxRetries: Int = Try(config.getInt(EsHighLevelConfigKeys.CONNECTION_TIMEOUT)).getOrElse(0)
  val retryDelayFactor: Int = Try(config.getInt(EsHighLevelConfigKeys.RETRY_DELAY_FACTOR)).getOrElse(1)
  val connectionTimeout: Int = Try(config.getInt(EsHighLevelConfigKeys.CONNECTION_TIMEOUT)).getOrElse(-1)
  val socketTimeout: Int = Try(config.getInt(EsHighLevelConfigKeys.SOCKET_TIMEOUT)).getOrElse(-1)
  val connectionRequestTimeout: Int = Try(config.getInt(EsHighLevelConfigKeys.CONNECTION_REQUEST_TIMEOUT)).getOrElse(-1)

  /*
   * bulk
   *****************************************************************************/

  /**
    * @return upper limit of number of changes triggering a database flush
    */
  def bulkActions: Int = config.getInt(EsHighLevelConfigKeys.BULK_ACTIONS)

  /**
    * @return maximum total size of changed documents triggering a database flush
    */
  def bulkSize: Long = config.getLong(EsHighLevelConfigKeys.BULK_SIZE)

  /**
    * @return maximum number of seconds after which a database flush is triggered
    */
  def flushInterval: Long = config.getLong(EsHighLevelConfigKeys.FLUSH_INTERVAL)

  /**
    * @return maximum number of concurrent requests (in terms of a connection pool)
    */
  def concurrentRequests: Int = config.getInt(EsHighLevelConfigKeys.CONCURRENT_REQUESTS)
}