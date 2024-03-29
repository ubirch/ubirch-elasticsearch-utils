package com.ubirch.util.elasticsearch

import com.ubirch.util.elasticsearch.config.EsHighLevelConfig
import org.apache.http.HttpHost
import org.apache.http.auth.{ AuthScope, UsernamePasswordCredentials }
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{ RestClient, RestClientBuilder, RestHighLevelClient }

/**
  * We recommend not to create multiple instances of this trait as it will create multiple connections
  * to the elasticsearch though it should be a singleton.
  */
trait EsHighLevelClient {

  private[elasticsearch] lazy val host = EsHighLevelConfig.host
  private[elasticsearch] lazy val port = EsHighLevelConfig.port
  private val scheme = EsHighLevelConfig.scheme
  private val user = EsHighLevelConfig.user
  private val password = EsHighLevelConfig.password
  private val connectionTimeout = EsHighLevelConfig.connectionTimeout
  private val socketTimeout = EsHighLevelConfig.socketTimeout
  private val connectionRequestTimeout = EsHighLevelConfig.connectionRequestTimeout
  private val maxConnPerRoute = EsHighLevelConfig.maxConnectionPerRoute
  private val maxConnTotal = EsHighLevelConfig.maxConnectionTotal

  private val credentialsProvider = new BasicCredentialsProvider()
  credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))

  private val builder = RestClient
    .builder(new HttpHost(host, port, scheme))
    .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback {
      override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder =
        requestConfigBuilder
          .setConnectTimeout(connectionTimeout)
          .setSocketTimeout(socketTimeout)
          .setConnectionRequestTimeout(connectionRequestTimeout)
    })

  if (user != "" && password != "")
    builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      override def customizeHttpClient(httpAsyncClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
        httpAsyncClientBuilder
          .setDefaultCredentialsProvider(credentialsProvider)
        if (maxConnPerRoute.isSuccess)
          httpAsyncClientBuilder.setMaxConnPerRoute(maxConnPerRoute.get)
        if (maxConnTotal.isSuccess)
          httpAsyncClientBuilder.setMaxConnTotal(maxConnTotal.get)
        httpAsyncClientBuilder
      }
    })

  val esClient: RestHighLevelClient = new RestHighLevelClient(builder)

}

object EsHighLevelClient extends EsHighLevelClient {
  final val client: RestHighLevelClient = esClient
}
