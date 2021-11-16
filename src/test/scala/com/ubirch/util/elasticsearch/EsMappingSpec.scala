package com.ubirch.util.elasticsearch

import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.GetIndexRequest

class EsMappingSpec extends TestUtils {

  feature("create index with mapping") {

    scenario("testIndex") {
      esMappingImpl.createElasticsearchMappings()
      val request = new GetIndexRequest(docIndex)
      assert(client.indices().exists(request, RequestOptions.DEFAULT), true)
    }
  }

}

