package com.ubirch.util.elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import co.elastic.clients.elasticsearch._types.mapping.KeywordProperty
import com.dimafeng.testcontainers.{ ElasticsearchContainer, ForAllTestContainer }
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.json.JsonFormats
import org.joda.time.DateTime
import org.json4s.Formats
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName

case class TestDoc(id: String, hello: String, value: Int, created: DateTime = DateTime.now)

trait TestUtils
  extends AsyncFeatureSpec
  with Matchers
  with BeforeAndAfterAll
  with StrictLogging
  with ForAllTestContainer {

  implicit protected val formats: Formats = JsonFormats.default
  protected val docIndex = "test-index"
  protected val defaultDocType = "_doc"
  protected var port: Int = _
  protected var host: String = _
  protected var esMappingImpl: TestEsMappingImpl = _
  protected var simpleClient: TestEsSimpleClient = _
  protected var bulkClient: TestEsBulkClient = _
  protected var client: ElasticsearchAsyncClient = _

  class TestEsSimpleClient(client: ElasticsearchAsyncClient) extends EsSimpleClientBase {
    override val esClient: ElasticsearchAsyncClient = client
  }

  class TestEsBulkClient(client: ElasticsearchAsyncClient) extends EsBulkClientBase {
    override val esClient: ElasticsearchAsyncClient = client
  }

  class TestEsAsyncClient(testHost: String, httpPort: Int) extends EsAsyncClient {
    override lazy val port: Int = httpPort
    override lazy val host: String = testHost
  }

  class TestEsMappingImpl(client: ElasticsearchAsyncClient) extends EsMappingTrait {
    override val esClient: ElasticsearchAsyncClient = client

    override val indexesAndMappings =
      Map(docIndex -> Map(
        "id" -> new KeywordProperty.Builder().build()._toProperty(),
        "hello" -> new KeywordProperty.Builder().build()._toProperty(),
        "value" -> new KeywordProperty.Builder().build()._toProperty()
      ))
  }

  override val container: ElasticsearchContainer =
    ElasticsearchContainer(
      DockerImageName
        .parse("elastic/elasticsearch:8.4.3")
        .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch:8.4.3")
    ).configure { c =>
      c.withEnv("xpack.security.enabled", "false")
    }

  override def beforeAll(): Unit = {
    val hostAndPort = container.httpHostAddress.split(":")
    host = hostAndPort(0)
    port = hostAndPort(1).toInt
    client = new TestEsAsyncClient(host, port).esClient
    simpleClient = new TestEsSimpleClient(client)
    esMappingImpl = new TestEsMappingImpl(client)
    bulkClient = new TestEsBulkClient(client)
  }

  override def afterAll(): Unit = {
    bulkClient.closeConnection()
    simpleClient.closeConnection()
    container.stop()
    container.close()
  }
}
