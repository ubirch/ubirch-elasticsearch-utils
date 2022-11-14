package com.ubirch.util.elasticsearch

import com.ubirch.util.elasticsearch.util.QueryUtil
import com.ubirch.util.json.Json4sUtil
import org.joda.time.DateTime

import scala.concurrent.Future

class EsBulkClientSpec extends TestUtils {

  val now = DateTime.now()
  val listOfDocs: Seq[TestDoc] = Range.inclusive(1, 10000).map { i =>
    TestDoc(i.toString, "World", 1 * i, now.plusDays(i))
  }

  Feature("simple CRUD tests") {

    Scenario("store 10000 documents and check if all stored correctly retrieved") {
      for {
        cleaned <- esMappingImpl.cleanElasticsearch()
        _ = assert(cleaned)
        idsAndDocs =
          listOfDocs
            .map(testDoc => testDoc.id -> Json4sUtil.any2jvalue(testDoc))
            .filter(_._2.isDefined)
            .map(r => r._1 -> r._2.get)
            .toMap
        r1 <- bulkClient.storeDocBulk(docIndex, idsAndDocs)
        _ = assert(r1.isInstanceOf[Unit])
        r2 <- Future.sequence(idsAndDocs.map { case (id, _) => simpleClient.getDoc(docIndex, id) })
        _ = assert(!r2.exists(_.isEmpty))
      } yield r2.size shouldBe 10000
    }

    Scenario("store 10000 documents and check if range query works") {

      for {
        cleaned <- esMappingImpl.cleanElasticsearch()
        _ = assert(cleaned)
        idsAndDocs =
          listOfDocs
            .map(testDoc => testDoc.id -> Json4sUtil.any2jvalue(testDoc))
            .filter(_._2.isDefined)
            .map(r => r._1 -> r._2.get)
            .toMap
        r1 <- bulkClient.storeDocBulk(docIndex, idsAndDocs, waitingForRefresh = true)
        _ = assert(r1.isInstanceOf[Unit])
        deviceIdStrings = listOfDocs.map(_.id).toSet
        from = listOfDocs.drop(1000).head.created
        to = listOfDocs.dropRight(1000).last.created
        queries = Seq(
          QueryUtil.buildTermsQuery("id", deviceIdStrings),
          QueryUtil.buildDateRangeQuery("created", from, to))
        query = Some(QueryUtil.buildBoolMustQuery(queries))
        r3 <- simpleClient.getDocs(docIndex, query, size = 10000)
      } yield r3.size shouldBe 8000
    }

  }
}
