package com.ubirch.util.elasticsearch

import com.typesafe.scalalogging.StrictLogging
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs
import org.elasticsearch.common.settings.Settings

object TestUtils extends StrictLogging {

  val clusterName = "elasticsearch"
  private val runner: ElasticsearchClusterRunner = new ElasticsearchClusterRunner().onBuild(new ElasticsearchClusterRunner.Builder() {
    override def build(i: Int, builder: Settings.Builder): Unit = { // put elasticsearch settings
      builder.put("discovery.type", "single-node")
    }
  })
  runner.build(newConfigs().numOfNode(1).clusterName(clusterName).baseHttpPort(9200))

  def start(): Unit = {
    try {
      runner.ensureYellow();
    } catch {
      case ex: Throwable =>
        logger.error("ES startup error: ", ex)
    }
  }


}
