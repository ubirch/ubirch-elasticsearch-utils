package com.ubirch.util.elasticsearch

import com.ubirch.util.elasticsearch.config.EsHighLevelConfig
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

trait EsHighLevelClient {

  private val host = EsHighLevelConfig.host
  private val port = EsHighLevelConfig.port
  private val scheme = EsHighLevelConfig.scheme
  private val user = EsHighLevelConfig.user
  private val password = EsHighLevelConfig.password
  private val connectionTimeout = EsHighLevelConfig.connectionTimeout
  private val socketTimeout = EsHighLevelConfig.socketTimeout
  private val retryTimeout = EsHighLevelConfig.retryTimeout

  private val credentialsProvider = new BasicCredentialsProvider()
  credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))

  private val builder = RestClient
    .builder(new HttpHost(host, port, scheme))
    .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback {
      override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder =
        requestConfigBuilder
          .setConnectTimeout(connectionTimeout)
          .setSocketTimeout(socketTimeout)
          .setConnectionRequestTimeout(retryTimeout)
    })

  if (user != "" && password != "")
    builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
        httpClientBuilder
          .setDefaultCredentialsProvider(credentialsProvider)
      }
    })

  val esClient: RestHighLevelClient = new RestHighLevelClient(builder)

}

object EsHighLevelClient extends EsHighLevelClient {
  final val client: RestHighLevelClient = esClient
}