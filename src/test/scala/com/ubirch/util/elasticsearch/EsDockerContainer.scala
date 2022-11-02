package com.ubirch.util.elasticsearch

import com.dimafeng.testcontainers.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName

object EsDockerContainer {
  val container: ElasticsearchContainer = ElasticsearchContainer(
    DockerImageName
      .parse("elastic/elasticsearch:8.4.3")
      .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch:8.4.3")
  ).configure { c =>
    c.withEnv("xpack.security.enabled", "true")
    c.withEnv("ELASTIC_USERNAME", "elastic")
    c.withEnv("ELASTIC_PASSWORD", "changeMe")
  }
  container.start()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      container.stop()
      container.close()
    }
  })

}
