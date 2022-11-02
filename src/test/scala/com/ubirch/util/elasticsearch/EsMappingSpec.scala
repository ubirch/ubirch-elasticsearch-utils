package com.ubirch.util.elasticsearch

import scala.concurrent.Promise

class EsMappingSpec extends TestUtils {

  Feature("create index with mapping") {

    Scenario("testIndex") {
      val promise = Promise[Boolean]()

      esMappingImpl.createElasticsearchMappings().map {
        case true =>
          val request = new co.elastic.clients.elasticsearch.indices.ExistsRequest.Builder().index(docIndex).build()

          client.indices().exists(request).whenComplete {
            case (_, ex) if ex != null =>
              promise.success(false)
            case (rsp, _) =>
              promise.success(rsp.value())
          }

        case false =>
          promise.success(false)
      }
      promise.future.map(r => assert(r))

    }
  }

}
