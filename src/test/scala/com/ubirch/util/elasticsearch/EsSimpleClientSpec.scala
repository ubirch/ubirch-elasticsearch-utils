package com.ubirch.util.elasticsearch

import com.ubirch.util.elasticsearch.util.{ESUtilExceptionV, QueryUtil, SortUtil}
import com.ubirch.util.json.Json4sUtil
import com.ubirch.util.uuid.UUIDUtil
import org.json4s._

import scala.language.postfixOps

/**
  * author: L. Rueger
  * since: 2016-10-06
  */
class EsSimpleClientSpec extends TestUtils {

  private val testDoc = TestDoc("1", "World", 10)
  private val testDoc2 = TestDoc("2", "Galaxy", 20)
  private val testDoc2Updated = TestDoc("2", "Galaxy-World", 15)
  private val testDoc3 = TestDoc("3", "Any-World", 5)

  Feature("simple CRUD tests") {

    Scenario("store") {
      val jval = Json4sUtil.any2jvalue(testDoc).get

      simpleClient
        .storeDoc(
          docIndex = docIndex,
          docIdOpt = Some(testDoc.id),
          doc = jval
        ).map { success =>
        success shouldBe()
        }
    }

    Scenario("failed get") {
      simpleClient.getDoc(docIndex, UUIDUtil.uuidStr).map(_.isDefined shouldBe false)
    }

    Scenario("store and get") {
      val jval = Json4sUtil.any2jvalue(testDoc).get

      simpleClient.storeDoc(
        docIndex = docIndex,
        docIdOpt = Some(testDoc.id),
        doc = jval,
        waitingForRefresh = true).flatMap { success =>
        success shouldBe()

        simpleClient.getDoc(docIndex, testDoc.id).map {
          case Some(jval) =>
            logger.debug(s"fetched some document")
            val rTestDoc = jval.extract[TestDoc]
            rTestDoc.id shouldBe testDoc.id
            rTestDoc.hello shouldBe testDoc.hello
          case None =>
            fail("could not fetch document")
        }
      }
    }

    Scenario("update") {
      val jval = Json4sUtil.any2jvalue(testDoc2).get
      for {
        _ <- simpleClient.storeDoc(docIndex, jval, Some(testDoc2.id), waitingForRefresh = true)
        jValueOpt <- simpleClient.getDoc(docIndex, testDoc2.id)
        _ = {
          jValueOpt.isDefined shouldBe true
          val jValue = jValueOpt.get
          val rTestDoc = jValue.extract[TestDoc]
          rTestDoc.id shouldBe testDoc2.id
          rTestDoc.hello shouldBe testDoc2.hello
          rTestDoc.value shouldBe testDoc2.value
        }
        jValueUpdate = Json4sUtil.any2jvalue(testDoc2Updated).get
        _ <- simpleClient.storeDoc(docIndex, jValueUpdate, Some(testDoc2Updated.id), waitingForRefresh = true)
        jValueOpt2 <- simpleClient.getDoc(docIndex, testDoc2Updated.id)
      } yield {
        jValueOpt2.isDefined shouldBe true
        val jValue = jValueOpt2.get
        val rTestDoc = jValue.extract[TestDoc]
        rTestDoc.id shouldBe testDoc2Updated.id
        rTestDoc.hello shouldBe testDoc2Updated.hello
        rTestDoc.value shouldBe testDoc2Updated.value
      }
    }

    Scenario("getDocs with id") {
      for {
        _ <- simpleClient.storeDoc(
          docIndex,
          Json4sUtil.any2jvalue(testDoc3).get,
          Some(testDoc3.id),
          waitingForRefresh = true)
        query = Some(QueryUtil.buildTermQuery("id", testDoc3.id))
        r <- simpleClient.getDocs(docIndex, query)
      } yield r.size shouldBe 1
    }

    Scenario("getDocs with sort") {
      simpleClient.getDocs(docIndex, sort = Seq(SortUtil.buildSortOptions("value")))
        .map { r =>
          r.size shouldBe 3
          r.map(_.extract[TestDoc].id) shouldBe Seq(testDoc, testDoc2Updated, testDoc3).sortBy(_.value).map(_.id)
        }
    }

    Scenario("getDocs fails when index does not exist") {
      for {
        _ <- esMappingImpl.deleteIndices()
        r <- simpleClient.getDocs(docIndex)
          .map(_ => fail("should never reach here"))
          .recover(_.isInstanceOf[ESUtilExceptionV] shouldBe true)
      } yield r
    }

    Scenario("delete") {
      for {
        _ <- simpleClient.storeDoc(
          docIndex,
          Json4sUtil.any2jvalue(testDoc3).get,
          Some(testDoc3.id),
          waitingForRefresh = true)
        _ <- simpleClient.deleteDoc(
          docIndex,
          "id",
          testDoc3.id,
          waitingForRefresh = true)
        query = Some(QueryUtil.buildTermQuery("id", testDoc3.id))
        r <- simpleClient.getDocs(docIndex, query)
      } yield r.size shouldBe 0
    }

    Scenario("delete succeeds when id does not exist") {
      for {
        _ <- esMappingImpl.createElasticsearchMappings()
        _ <- simpleClient.storeDoc(
          docIndex,
          Json4sUtil.any2jvalue(testDoc3).get,
          Some(testDoc3.id),
          waitingForRefresh = true)
        _ <- simpleClient.deleteDoc(
          docIndex,
          "id",
          UUIDUtil.uuidStr,
          waitingForRefresh = true)
        r <- simpleClient.getDocs(docIndex)
      } yield r.size shouldBe 1
    }

    Scenario("delete when index does not exist") {
      for {
        _ <- esMappingImpl.deleteIndices()
        r <- simpleClient.deleteDoc(
          docIndex,
          "id",
          UUIDUtil.uuidStr)
          .map(_ => fail("should never reach here"))
          .recover(_.isInstanceOf[ESUtilExceptionV] shouldBe true)
      } yield r
    }

  }
}
