package com.ubirch.util.elasticsearch

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.json.{Json4sUtil, JsonFormats}
import com.ubirch.util.uuid.UUIDUtil
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.json4s._
import org.scalatest.{AsyncFeatureSpec, BeforeAndAfterAll, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * author: L. Rueger
  * since: 2016-10-06
  */
class EsSimpleClientSpec extends AsyncFeatureSpec
  with Matchers
  with BeforeAndAfterAll
  with StrictLogging {

  implicit private val formats: Formats = JsonFormats.default

  case class TestDoc(id: String, hello: String, value: Int)

  private val docIndex = "test-index"
  private val testDoc = TestDoc("1", "World", 10)
  private val testDoc2 = TestDoc("2", "Galaxy", 20)
  private val testDoc2Updated = TestDoc("2", "Galaxy-World", 10)

  private var esMappingImpl: EsMappingImpl = _

  implicit var esClient: RestHighLevelClient = _

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
    esClient = EsSimpleClient.getCurrentEsClient
  }


  feature("simple CRUD tests") {

    scenario("store") {
      val jval = Json4sUtil.any2jvalue(testDoc).get

      EsSimpleClient.storeDoc(
        docIndex = docIndex,
        docIdOpt = Some(testDoc.id),
        doc = jval).map { success =>
        success shouldBe true
      }
    }

    scenario("failed get") {
      EsSimpleClient.getDoc(docIndex, UUIDUtil.uuidStr).map {
        _.isDefined shouldBe false
      }
    }

    scenario("store and get") {

      Thread.sleep(1500)
      EsSimpleClient.getDoc(docIndex, testDoc.id).map {
        case Some(jval) =>
          logger.debug("fetched some document")
          val rTestDoc = jval.extract[TestDoc]
          rTestDoc.id shouldBe testDoc.id
          rTestDoc.hello shouldBe testDoc.hello
        case None =>
          fail("could not fetch document")
      }
    }

    scenario("update") {
      val jval = Json4sUtil.any2jvalue(testDoc2).get
      Await.ready(EsSimpleClient.storeDoc(
        docIndex = docIndex,
        docIdOpt = Some(testDoc2.id),
        doc = jval), 2 seconds)
      Thread.sleep(1500)

      EsSimpleClient.getDoc(docIndex, testDoc2.id).map {
        case Some(jValue) =>
          val rTestDoc = jValue.extract[TestDoc]
          rTestDoc.id shouldBe testDoc2.id
          rTestDoc.hello shouldBe testDoc2.hello
          rTestDoc.value shouldBe testDoc2.value
        case None => fail("could not read stored document")
      }

      val jvalUpdate = Json4sUtil.any2jvalue(testDoc2Updated).get
      Await.ready(EsSimpleClient.storeDoc(
        docIndex = docIndex,
        docIdOpt = Some(testDoc2Updated.id),
        doc = jvalUpdate), 2 seconds)
      Thread.sleep(1500)

      EsSimpleClient.getDoc(docIndex, testDoc2Updated.id).map {
        case Some(jValue) =>
          val rTestDoc = jValue.extract[TestDoc]
          rTestDoc.id shouldBe testDoc2Updated.id
          rTestDoc.hello shouldBe testDoc2Updated.hello
          rTestDoc.value shouldBe testDoc2Updated.value
        case None => fail("could not read stored document")
      }
    }

    scenario("getDocs with id") {
      Thread.sleep(1000)

      val query = Some(QueryBuilders.termQuery("id", testDoc2Updated.id))
      EsSimpleClient.getDocs(docIndex, query = query).map {
        case jvals: List[JValue] =>
          jvals.size shouldBe 1
        case _ => fail("could not read stored document")
      }
    }

    scenario("getDocs") {
      EsSimpleClient.getDocs(docIndex).map {
        case jvals: List[JValue] =>
          jvals.size shouldBe 2
        case _ => fail("could not read stored document")
      }
    }

    scenario("getAverage() of existing field --> Some") {

      val aggregation: AvgAggregationBuilder =
        AggregationBuilders
          .avg("average")
          .field("value")

      EsSimpleClient.getAverage(
        docIndex = docIndex,
        avgAgg = aggregation
      ) map { result =>

        result shouldBe Some(10d)

      }

    }

    scenario("getAverage() of non-existing field --> None") {

      val aggregation: AvgAggregationBuilder =
        AggregationBuilders
          .avg("average")
          .field("NonExistingField")

      EsSimpleClient.getAverage(
        docIndex = docIndex,
        avgAgg = aggregation
      ) map { result =>
        result shouldBe None
      }

    }

    scenario("delete") {
      val jval = Json4sUtil.any2jvalue(testDoc).get
      EsSimpleClient.storeDoc(
        docIndex = docIndex,
        docIdOpt = Some(testDoc.id),
        doc = jval).map { success =>
        success shouldBe true
      }

      EsSimpleClient.deleteDoc(docIndex, testDoc.id).map { res =>
        res shouldBe true
      }
    }
  }


}