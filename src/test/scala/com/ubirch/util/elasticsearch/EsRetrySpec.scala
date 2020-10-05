package com.ubirch.util.elasticsearch

import java.io.IOException

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.json.{Json4sUtil, JsonFormats}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.json4s.{Formats, JValue}
import org.scalatest.{AsyncFeatureSpec, BeforeAndAfterAll, Matchers}

import scala.concurrent.Promise

class EsRetrySpec extends AsyncFeatureSpec with EsMappingTrait
  with Matchers
  with BeforeAndAfterAll
  with StrictLogging {

  implicit private val formats: Formats = JsonFormats.default

  implicit val esClient: RestHighLevelClient = EsSimpleClient.getCurrentEsClient

  val docIndex = "test-index"
  val defaultDocType = "_doc"

  case class TestDoc(id: String, hello: String, value: Int)

  private val testDoc = TestDoc("1", "World", 10)
  val jValue: JValue = Json4sUtil.any2jvalue(testDoc).get

  class MockIOExceptionEsSimpleClient(retries: Int) extends EsSimpleClientBase {
    var counter = 0
    override val maxRetries: Int = retries

    override def createActionListener[T](promise: Promise[T]): ActionListener[T] = {
      counter += 1
      new ActionListener[T] {

        override def onResponse(response: T): Unit = promise.failure(new IOException("test exception handling"))

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    }
  }

  class MockThrowableEsSimpleClient(retries: Int) extends EsSimpleClientBase {
    var counter = 0
    override val maxRetries: Int = retries

    override def createActionListener[T](promise: Promise[T]): ActionListener[T] = {
      counter += 1
      new ActionListener[T] {

        override def onResponse(response: T): Unit = promise.failure(new Throwable("test throwable handling"))

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    }

  }


  override protected def beforeAll(): Unit = {
    cleanElasticsearch()
  }

  feature("simple retry") {

    scenario("fail with no retry") {

      val mockClient = new MockIOExceptionEsSimpleClient(0)

      recoverToExceptionIf[IOException] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { e =>
        e.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 1
      }
    }

    scenario("fail with two retries") {

      val mockClient = new MockIOExceptionEsSimpleClient(2)

      recoverToExceptionIf[IOException] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 3
      }

    }

  }

  feature("simple failure when unknown exception") {

    scenario("fail with no retry") {

      val mockClient = new MockThrowableEsSimpleClient(0)

      recoverToExceptionIf[Throwable] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { e =>
        e.getMessage shouldBe "test throwable handling"
        mockClient.counter shouldBe 1
      }
    }

    scenario("fail with no retry also though maxRetries > 0") {

      val mockClient = new MockThrowableEsSimpleClient(2)

      recoverToExceptionIf[Throwable] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { ex =>
        ex.getMessage shouldBe "test throwable handling"
        mockClient.counter shouldBe 1
      }
    }
  }

  feature("fail all remaining methods with one retry") {
    val mockClient = new MockIOExceptionEsSimpleClient(1)

    scenario("getDoc") {
      recoverToExceptionIf[IOException] {
        mockClient.getDoc(docIndex, testDoc.id)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 2
      }
    }

    scenario("getDocs") {
      val query = Some(QueryBuilders.termQuery("id", testDoc.id))

      recoverToExceptionIf[IOException] {
        mockClient.getDocs(docIndex, query = query)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 4
      }
    }


    scenario("getAverage() of existing field --> Some") {

      val aggregation = AggregationBuilders.avg("average").field("value")

      recoverToExceptionIf[IOException] {
        mockClient.getAverage(docIndex = docIndex, avgAgg = aggregation)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 6
      }
    }

    scenario("delete") {
      recoverToExceptionIf[IOException] {
        mockClient.deleteDoc(docIndex, testDoc.id)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 8
      }
    }

    scenario("connectivityCheck with existing index") {

      mockClient.connectivityCheck(docIndex).map { _ =>
        mockClient.counter shouldBe 10
      }
    }

  }


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