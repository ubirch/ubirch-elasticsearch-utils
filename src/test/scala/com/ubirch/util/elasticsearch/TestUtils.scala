package com.ubirch.util.elasticsearch

import com.dimafeng.testcontainers.{ ElasticsearchContainer, ForAllTestContainer }
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.json.JsonFormats
import org.elasticsearch.client.RestHighLevelClient
import org.json4s.Formats
import org.scalatest.{ AsyncFeatureSpec, BeforeAndAfterAll, Matchers }
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

  class TestEsHighLevelClient(httpPort: Int) extends EsHighLevelClient {
    override lazy val port: Int = httpPort
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
    port = container.httpHostAddress.replaceAll("\\D+", "").toInt
    client = new TestEsHighLevelClient(port).esClient
    esMappingImpl = new TestEsMappingImpl(client)
    simpleClient = new TestEsSimpleClient(client)
    bulkClient = new TestEsBulkClient(client)
  }

}
