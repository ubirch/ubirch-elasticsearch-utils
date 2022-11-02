package com.ubirch.util.elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import com.typesafe.scalalogging.Logger
import com.ubirch.util.elasticsearch.util.{ ESUtilException, QueryUtil, ResultUtil }
import com.ubirch.util.json.Json4sUtil
import org.json4s.JValue

import scala.concurrent.Promise

class EsRetrySpec extends TestUtils {

  private val testDoc = TestDoc("1", "World", 10)
  private val jValue: JValue = Json4sUtil.any2jvalue(testDoc).get

  class MockESUtilExceptionEsSimpleClient(retries: Int, client: ElasticsearchAsyncClient)
    extends TestEsSimpleClient(client) {
    var counter = 0
    override val maxRetries: Int = retries

    class ResultUtilMock() extends ResultUtil() {

      override def handleSuccess[T](p: Promise[T], r: T, msg: String)(implicit logger: Logger): Promise[T] = {
        counter += 1
        logger.info("ES success, " + msg)
        p.failure(ESUtilException("mock exception"))
      }
    }

    override val r = new ResultUtilMock()
  }

  Feature("simple retry") {

    Scenario("fail with no retry") {
      val mockClient = new MockESUtilExceptionEsSimpleClient(0, client)

      recoverToExceptionIf[ESUtilException] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { e =>
        e.getMessage shouldBe "mock exception"
        mockClient.counter shouldBe 1
      }
    }

    Scenario("fail with two retries") {
      val mockClient = new MockESUtilExceptionEsSimpleClient(2, client)
      recoverToExceptionIf[ESUtilException] {
        mockClient.storeDoc(docIndex = docIndex, docIdOpt = Some(testDoc.id), doc = jValue)
      }.map { ex =>
        ex.getMessage shouldBe "mock exception"
        mockClient.counter shouldBe 3
      }
    }
  }

  Feature("fail all remaining methods with one retry") {

    Scenario("getDoc") {
      val mockClient = new MockESUtilExceptionEsSimpleClient(1, client)
      recoverToExceptionIf[ESUtilException] {
        mockClient.getDoc(docIndex, testDoc.id)
      }.map { ex =>
        ex.getMessage shouldBe "mock exception"
        mockClient.counter shouldBe 2
      }
    }

    Scenario("getDocs") {
      val mockClient = new MockESUtilExceptionEsSimpleClient(1, client)
      val query = Some(QueryUtil.buildTermQuery("id", testDoc.id))
      recoverToExceptionIf[ESUtilException] {
        mockClient.getDocs(docIndex, query = query)
      }.map { ex =>
        ex.getMessage shouldBe "mock exception"
        mockClient.counter shouldBe 2
      }
    }

    Scenario("delete") {
      val mockClient = new MockESUtilExceptionEsSimpleClient(1, client)
      recoverToExceptionIf[ESUtilException] {
        mockClient.deleteDoc(docIndex, "id", testDoc.id)
      }.map { ex =>
        ex.getMessage shouldBe "mock exception"
        mockClient.counter shouldBe 2
      }
    }

    Scenario("connectivityCheck with existing index") {
      val mockClient = new MockESUtilExceptionEsSimpleClient(1, client)
      mockClient.connectivityCheck(docIndex).map { _ =>
        mockClient.counter shouldBe 2
      }
    }
  }
}
