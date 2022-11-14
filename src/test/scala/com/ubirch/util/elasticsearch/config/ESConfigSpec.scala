package com.ubirch.util.elasticsearch.config

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class ESConfigSpec extends AnyFeatureSpec with Matchers {

  Feature("hosts()") {

    Scenario("read list of hosts from config") {

      // verify
      EsAsyncClientConfig.host shouldBe "localhost"
      EsAsyncClientConfig.port shouldBe 9201
      EsAsyncClientConfig.scheme shouldBe "http"
      EsAsyncClientConfig.user shouldBe "elastic"
      EsAsyncClientConfig.password shouldBe "changeMe"
      EsAsyncClientConfig.connectionTimeout shouldBe -1
      EsAsyncClientConfig.socketTimeout shouldBe -1
      EsAsyncClientConfig.connectionRequestTimeout shouldBe -1
    }
  }

  Feature("bulk()") {

    Scenario("read bulk settings from config") {

      // verify
      EsAsyncClientConfig.bulkActions shouldBe 10000
      EsAsyncClientConfig.bulkSize shouldBe 1
      EsAsyncClientConfig.flushInterval shouldBe 1
      EsAsyncClientConfig.concurrentRequests shouldBe 2
    }
  }

}
