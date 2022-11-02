package com.ubirch.util.elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.ubirch.util.elasticsearch.config.EsAsyncClientConfig
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{RestClient, RestClientBuilder}

/**
  * We recommend not to create multiple instances of this trait as it will create multiple connections
  * to the elasticsearch though it should be a singleton.
  */
trait EsAsyncClient {

  private[elasticsearch] lazy val host = EsAsyncClientConfig.host
  private[elasticsearch] lazy val port = EsAsyncClientConfig.port
  private val scheme = EsAsyncClientConfig.scheme
  private val user = EsAsyncClientConfig.user
  private val password = EsAsyncClientConfig.password
  private val connectionTimeout = EsAsyncClientConfig.connectionTimeout
  private val socketTimeout = EsAsyncClientConfig.socketTimeout
  private val connectionRequestTimeout = EsAsyncClientConfig.connectionRequestTimeout
  private val maxConnPerRoute = EsAsyncClientConfig.maxConnectionPerRoute
  private val maxConnTotal = EsAsyncClientConfig.maxConnectionTotal

  private val credentialsProvider = new BasicCredentialsProvider()
  credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))

  // Create the low-level client
  private val builder = RestClient
    .builder(new HttpHost(host, port, scheme))
    .setRequestConfigCallback((requestConfigBuilder: RequestConfig.Builder) =>
      requestConfigBuilder
        .setConnectTimeout(connectionTimeout)
        .setSocketTimeout(socketTimeout)
        .setConnectionRequestTimeout(connectionRequestTimeout))

  if (user != "" && password != "") {
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
  }

  val restClient: RestClient = builder.build()

  // Create the transport with a Jackson mapper
  val transport = new RestClientTransport(restClient, new JacksonJsonpMapper())

  // And create the API client
  val esClient = new ElasticsearchAsyncClient(transport)

}

object EsAsyncClient extends EsAsyncClient {
  final val asyncClient = esClient
}
