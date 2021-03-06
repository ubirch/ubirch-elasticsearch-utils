package com.ubirch.util.elasticsearch

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.json.Json4sUtil
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.scalatest.{AsyncFeatureSpec, BeforeAndAfterAll, Matchers}

class EsBulkClientSpec extends AsyncFeatureSpec
  with Matchers
  with BeforeAndAfterAll
  with StrictLogging {

  val docIndex = "test-index"

  val defaultDocType = "_doc"
  var esMappingImpl: EsMappingImpl = _

  implicit var client: RestHighLevelClient = _

  class EsMappingImpl extends EsMappingTrait {
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

  override def beforeAll(): Unit = {
    TestUtils.start()
    esMappingImpl = new EsMappingImpl()
    client = EsSimpleClient.getCurrentEsClient
  }

  case class TestDoc(id: String, hello: String, value: Int)

  val listOfDocs: Seq[TestDoc] = Range(1, 1999).map { int => TestDoc(int.toString, "World", 1 * int) }

  feature("simple CRUD tests") {

    scenario("store 2000 documents and check if average is good") {
      esMappingImpl.cleanElasticsearch()
      listOfDocs.foreach { testDoc =>

        val jval = Json4sUtil.any2jvalue(testDoc).get

        EsBulkClient.storeDocBulk(
          docIndex = docIndex,
          docId = testDoc.id,
          doc = jval)
      }
      Thread.sleep(3000)

      val aggregation: AvgAggregationBuilder =
        AggregationBuilders
          .avg("average")
          .field("value")

      EsSimpleClient.getAverage(
        docIndex = docIndex,
        avgAgg = aggregation
      ) map { result =>
        result shouldBe Some(999.5d)
      }
    }

  }

}
