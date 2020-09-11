// see http://www.scala-sbt.org/0.13/docs/Parallel-Execution.html for details
concurrentRestrictions in Global := Seq(
  Tags.limit(Tags.Test, 1)
)

/*
 * BASIC INFORMATION
 ********************************************************/

name := "ubirch-elasticsearch-utils"
description := "Elasticsearch client using the High Level Java Client"
version := "0.2.3"
organization := "com.ubirch.util"
homepage := Some(url("http://ubirch.com"))
scalaVersion := "2.11.12"
scalacOptions ++= Seq(
  "-feature"
)
scmInfo := Some(ScmInfo(
  url("https://github.com/ubirch/ubirch-elasticsearch-utils"),
  "https://github.com/ubirch/ubirch-elasticsearch-utils.git"
))

/*
 * CREDENTIALS
 ********************************************************/

(sys.env.get("CLOUDREPO_USER"), sys.env.get("CLOUDREPO_PW")) match {
  case (Some(username), Some(password)) =>
    println("USERNAME and/or PASSWORD found.")
    credentials += Credentials("ubirch.mycloudrepo.io", "ubirch.mycloudrepo.io", username, password)
  case _ =>
    println("USERNAME and/or PASSWORD is taken from /.sbt/.credentials")
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
}

/*
 * RESOLVER
 ********************************************************/

val resolverUbirchUtils = "ubirch.mycloudrepo.io" at "https://ubirch.mycloudrepo.io/repositories/ubirch-utils-mvn"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  resolverUbirchUtils)


/*
 * PUBLISHING
 ********************************************************/


publishTo := Some(resolverUbirchUtils)
publishMavenStyle := true


/*
 * DEPENDENCIES
 ********************************************************/


// Versions
val json4sV = "3.6.0"
val elasticsearchV = "7.8.0"

// Groups
val ubirchUtilGroup = "com.ubirch.util"

//Ubirch dependencies
lazy val ubirchUtilConfig = ubirchUtilGroup %% "ubirch-config-utils" % "0.2.4"
lazy val ubirchUtilDeepCheckModel = ubirchUtilGroup %% "ubirch-deep-check-utils" % "0.4.1"
lazy val ubirchUtilJson = ubirchUtilGroup %% "ubirch-json-utils" % "0.5.2"
lazy val ubirchUtilUuid = ubirchUtilGroup %% "ubirch-uuid-utils" % "0.1.4"

//External dependencies
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
val elasticSearch = "org.elasticsearch" % "elasticsearch" % elasticsearchV
val elasticSearchClient = "org.elasticsearch.client" % "elasticsearch-rest-client" % elasticsearchV
val elasticSearchHighLevelClient = "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % elasticsearchV

// https://mvnrepository.com/artifact/io.monix/monix-execution
val monixExecution = "io.monix" %% "monix-execution" % "3.2.2"



lazy val json4sBase = Seq(
  json4sCore,
  json4sJackson,
  json4sExt
)
lazy val json4sWithNative = json4sBase

lazy val json4sJackson = "org.json4s" %% "json4s-jackson" % json4sV
lazy val json4sCore = "org.json4s" %% "json4s-core" % json4sV
lazy val json4sExt = "org.json4s" %% "json4s-ext" % json4sV

lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
lazy val slf4j = "org.slf4j" % "slf4j-api" % "1.7.21"
lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.1.7"

lazy val depSlf4jLogging = Seq(
  scalaLogging,
  slf4j,
  logbackClassic
)


libraryDependencies ++= Seq(
  monixExecution,
  elasticSearch,
  elasticSearchClient,
  elasticSearchHighLevelClient,
  ubirchUtilJson,
  ubirchUtilUuid,
  ubirchUtilDeepCheckModel,
  ubirchUtilConfig,
  scalaTest % "test"
) ++ json4sBase ++ depSlf4jLogging
