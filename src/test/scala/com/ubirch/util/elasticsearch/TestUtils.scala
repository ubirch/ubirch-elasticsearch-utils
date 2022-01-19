package com.ubirch.util.elasticsearch

import com.dimafeng.testcontainers.{ ElasticsearchContainer, ForAllTestContainer }
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.json.JsonFormats
import org.elasticsearch.client.RestHighLevelClient
import org.json4s.Formats
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AsyncFeatureSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName

case class TestDoc(id: String, hello: String, value: Int)

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
  protected var client: RestHighLevelClient = _

  class TestEsSimpleClient(client: RestHighLevelClient) extends EsSimpleClientBase {
    override val esClient: RestHighLevelClient = client
  }

  class TestEsBulkClient(client: RestHighLevelClient) extends EsBulkClientBase {
    override val esClient: RestHighLevelClient = client
  }

  class TestEsHighLevelClient(testHost: String, httpPort: Int) extends EsHighLevelClient {
    override lazy val port: Int = httpPort
    override lazy val host: String = testHost
  }

  class TestEsMappingImpl(client: RestHighLevelClient) extends EsMappingTrait {
    override val esClient: RestHighLevelClient = client

    override val indexesAndMappings: Map[String, String] =
      Map(docIndex ->
        s"""{
           |    "properties" : {
           |      "id" : {
           |        "type" : "keyword"
           |      },
           |      "hello" : {
           |        "type" : "keyword"
           |      },
           |      "value" : {
           |        "type" : "integer"
           |      }
           |    }
           |}""".stripMargin)
  }

  override val container: ElasticsearchContainer =
    ElasticsearchContainer(
      DockerImageName
        .parse("elastic/elasticsearch:7.15.0")
        .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch:7.15.0")
    ).configure { c =>
      c.withEnv("xpack.security.enabled", "false")
    }

  override def beforeAll(): Unit = {
    val hostAndPort = container.httpHostAddress.split(":")
    host = hostAndPort(0)
    port = hostAndPort(1).toInt
    client = new TestEsHighLevelClient(host, port).esClient
    esMappingImpl = new TestEsMappingImpl(client)
    simpleClient = new TestEsSimpleClient(client)
    bulkClient = new TestEsBulkClient(client)
  }

  override def afterAll(): Unit = {
    bulkClient.closeConnection()
    simpleClient.closeConnection()
    container.stop()
    container.close()
  }
}
