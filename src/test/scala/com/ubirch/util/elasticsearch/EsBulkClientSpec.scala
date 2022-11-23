package com.ubirch.util.elasticsearch

import com.ubirch.util.json.Json4sUtil
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder

class EsBulkClientSpec extends TestUtils {

  val listOfDocs: Seq[TestDoc] = Range(1, 1999).map { int => TestDoc(int.toString, "World", 1 * int) }

  Feature("simple CRUD tests") {

    Scenario("store 2000 documents and check if average is good") {
      esMappingImpl.cleanElasticsearch()
      listOfDocs.foreach { testDoc =>
        val jval = Json4sUtil.any2jvalue(testDoc).get
        bulkClient.storeDocBulk(
          docIndex = docIndex,
          docId = testDoc.id,
          doc = jval)
      }
      Thread.sleep(3000)

      val aggregation: AvgAggregationBuilder =
        AggregationBuilders
          .avg("average")
          .field("value")

      simpleClient.getAverage(
        docIndex = docIndex,
        avgAgg = aggregation
      ) map { result =>
        result shouldBe Some(999.5d)
      }
    }

  }
}
