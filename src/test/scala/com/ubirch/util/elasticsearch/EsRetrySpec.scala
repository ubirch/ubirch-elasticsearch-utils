package com.ubirch.util.elasticsearch

import com.ubirch.util.json.Json4sUtil
import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.json4s.JValue

import java.io.IOException
import scala.concurrent.Promise

class EsRetrySpec extends TestUtils {

  private val testDoc = TestDoc("1", "World", 10)
  private val jValue: JValue = Json4sUtil.any2jvalue(testDoc).get

  class MockIOExceptionEsSimpleClient(retries: Int, client: RestHighLevelClient) extends TestEsSimpleClient(client) {
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

  class MockThrowableEsSimpleClient(retries: Int, client: RestHighLevelClient) extends TestEsSimpleClient(client) {
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

  Feature("simple retry") {

    Scenario("fail with no retry") {
      val mockClient = new MockIOExceptionEsSimpleClient(0, client)
      recoverToExceptionIf[IOException] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { e =>
        e.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 1
      }
    }

    Scenario("fail with two retries") {
      val mockClient = new MockIOExceptionEsSimpleClient(2, client)
      recoverToExceptionIf[IOException] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 3
      }
    }
  }

  Feature("simple failure when unknown exception") {

    Scenario("fail with no retry") {
      val mockClient = new MockThrowableEsSimpleClient(0, client)
      recoverToExceptionIf[Throwable] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { e =>
        e.getMessage shouldBe "test throwable handling"
        mockClient.counter shouldBe 1
      }
    }

    Scenario("fail with no retry also though maxRetries > 0") {
      val mockClient = new MockThrowableEsSimpleClient(2, client)
      recoverToExceptionIf[Throwable] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { ex =>
        ex.getMessage shouldBe "test throwable handling"
        mockClient.counter shouldBe 1
      }
    }
  }

  Feature("fail all remaining methods with one retry") {

    Scenario("getDoc") {
      val mockClient = new MockIOExceptionEsSimpleClient(1, client)
      recoverToExceptionIf[IOException] {
        mockClient.getDoc(docIndex, testDoc.id)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 2
      }
    }

    Scenario("getDocs") {
      val mockClient = new MockIOExceptionEsSimpleClient(1, client)
      val query = Some(QueryBuilders.termQuery("id", testDoc.id))
      recoverToExceptionIf[IOException] {
        mockClient.getDocs(docIndex, query = query)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 2
      }
    }

    Scenario("getAverage() of existing field --> Some") {
      val mockClient = new MockIOExceptionEsSimpleClient(1, client)
      val aggregation = AggregationBuilders.avg("average").field("value")
      recoverToExceptionIf[IOException] {
        mockClient.getAverage(docIndex = docIndex, avgAgg = aggregation)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 2
      }
    }

    Scenario("delete") {
      val mockClient = new MockIOExceptionEsSimpleClient(1, client)
      recoverToExceptionIf[IOException] {
        mockClient.deleteDoc(docIndex, testDoc.id)
      }.map { ex =>
        ex.getMessage shouldBe "test exception handling"
        mockClient.counter shouldBe 2
      }
    }

    Scenario("connectivityCheck with existing index") {
      val mockClient = new MockIOExceptionEsSimpleClient(1, client)
      mockClient.connectivityCheck(docIndex).map { _ =>
        mockClient.counter shouldBe 2
      }
    }
  }

}
