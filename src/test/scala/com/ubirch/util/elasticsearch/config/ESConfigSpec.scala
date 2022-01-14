package com.ubirch.util.elasticsearch.config

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class ESConfigSpec extends AnyFeatureSpec with Matchers {

  Feature("hosts()") {

    Scenario("read list of hosts from config") {

      // verify
      EsHighLevelConfig.host shouldBe "localhost"
      EsHighLevelConfig.port shouldBe 9201
      EsHighLevelConfig.scheme shouldBe "http"
      EsHighLevelConfig.user shouldBe ""
      EsHighLevelConfig.password shouldBe ""
      EsHighLevelConfig.connectionTimeout shouldBe -1
      EsHighLevelConfig.socketTimeout shouldBe -1
      EsHighLevelConfig.connectionRequestTimeout shouldBe -1
    }
  }

  Feature("bulk()") {

    Scenario("read bulk settings from config") {

      // verify
      EsHighLevelConfig.bulkActions shouldBe 10000
      EsHighLevelConfig.bulkSize shouldBe 1
      EsHighLevelConfig.flushInterval shouldBe 1
      EsHighLevelConfig.concurrentRequests shouldBe 2
    }
  }

}
