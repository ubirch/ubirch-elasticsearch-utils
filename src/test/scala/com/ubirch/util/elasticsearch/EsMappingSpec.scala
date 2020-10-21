package com.ubirch.util.elasticsearch

import com.typesafe.scalalogging.StrictLogging
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.scalatest.{AsyncFeatureSpec, BeforeAndAfterAll, Matchers}

class EsMappingSpec extends AsyncFeatureSpec
  with Matchers
  with BeforeAndAfterAll
  with StrictLogging {

  private val testIndex = "test-index"
  implicit var esClient: RestHighLevelClient = _
  private var esMappingImpl: EsMappingImpl = _

  class EsMappingImpl extends EsMappingTrait {
    override val indexesAndMappings: Map[String, String] =
      Map(testIndex ->
        s"""{
           |  "properties" : {
           |    "id" : {
           |       "type" : "keyword"
           |    },
           |    "hello" : {
           |      "type" : "keyword"
           |    },
           |    "value" : {
           |      "type" : "integer"
           |     }
           |  }
           |}""".stripMargin)
  }

  override def beforeAll(): Unit = {
    TestUtils.start()
    esMappingImpl = new EsMappingImpl()
    esClient = EsSimpleClient.getCurrentEsClient
  }

  feature("create index with mapping") {

    scenario("testIndex") {
      esMappingImpl.createElasticsearchMappings()
      val request = new GetIndexRequest(testIndex)
      assert(esClient.indices().exists(request, RequestOptions.DEFAULT), true)
    }
  }

}

