package com.ubirch.util.elasticsearch.config

import org.scalatest.{FeatureSpec, Matchers}

class ESConfigSpec extends FeatureSpec with Matchers {

  feature("hosts()") {

    scenario("read list of hosts from config") {

      // verify
      EsHighLevelConfig.host shouldBe "localhost"
      EsHighLevelConfig.port shouldBe 9200
      EsHighLevelConfig.scheme shouldBe "http"
      EsHighLevelConfig.user shouldBe ""
      EsHighLevelConfig.password shouldBe ""
      EsHighLevelConfig.connectionTimeout shouldBe -1
      EsHighLevelConfig.socketTimeout shouldBe -1
      EsHighLevelConfig.retryTimeout shouldBe -1
    }
  }

  feature("bulk()") {

    scenario("read bulk settings from config") {

      // verify
      EsHighLevelConfig.bulkActions shouldBe 10000
      EsHighLevelConfig.bulkSize shouldBe 1
      EsHighLevelConfig.flushInterval shouldBe 1
      EsHighLevelConfig.concurrentRequests shouldBe 2
    }
  }


}
