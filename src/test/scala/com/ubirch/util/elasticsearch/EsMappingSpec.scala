package com.ubirch.util.elasticsearch

import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.GetIndexRequest

class EsMappingSpec extends TestUtils {

  Feature("create index with mapping") {

    Scenario("testIndex") {
      esMappingImpl.createElasticsearchMappings()
      val request = new GetIndexRequest(docIndex)
      assert(client.indices().exists(request, RequestOptions.DEFAULT), true)
    }
  }

}
