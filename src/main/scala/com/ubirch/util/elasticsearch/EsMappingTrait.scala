package com.ubirch.util.elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import co.elastic.clients.elasticsearch._types.mapping.{Property, TypeMapping}
import co.elastic.clients.elasticsearch.indices.{
  CreateIndexRequest,
  CreateIndexResponse,
  DeleteIndexRequest,
  DeleteIndexResponse,
  ExistsRequest
}
import co.elastic.clients.transport.endpoints.BooleanResponse
import com.typesafe.scalalogging.{Logger, StrictLogging}
import com.ubirch.util.elasticsearch.util.ResultUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.MapHasAsJava

trait EsMappingTrait extends StrictLogging {

  /**
    * All indices and their mappings (<b>OVERWRITE!!!</b>).
    *
    * A Map of indexes and optional mappings. The data is structured as follows:
    * <code>
    * Map(
    * "INDEX_1_NAME" -> Map (
    * "id" -> new KeywordProperty.Builder().build()._toProperty(),
    * "hello" -> new KeywordProperty.Builder().build()._toProperty(),
    * "timestamp" -> new DateProperty.Builder().format("strict_date_time").build()._toProperty()
    * ),
    * "INDEX_2_NAME" -> {...}
    * )
    * </code>
    */
  val indexesAndMappings: Map[String, Map[String, Property]]
  implicit val log: Logger = logger

  private[elasticsearch] val esClient: ElasticsearchAsyncClient = EsAsyncClient.asyncClient
  private[elasticsearch] val r = new ResultUtil()
  final lazy val indicesToDelete: Set[String] = indexesAndMappings.keys.toSet

  /**
    * Method to creates all indices and their mappings if not yet existing.
    */
  final def createElasticsearchMappings(): Future[Boolean] =
    Future.sequence(indexesAndMappings.map {
      case (index, indexMapping) => create(index, indexMapping)
    }).map(!_.toSet.contains(false))

  /**
    * Method that creates an index with it's mapping, if it doesn't exist yet.
    */
  private def create(index: String, map: Map[String, Property]): Future[Boolean] = {

    val p = Promise[Boolean]()
    val request = new ExistsRequest.Builder().index(index).build()

    esClient.indices().exists(request).whenCompleteAsync { (rsp: BooleanResponse, ex: Throwable) =>
      (rsp, ex) match {
        case (_, ex) if ex != null =>
          r.handleFailure(p, s"failed to check if index $index exists", ex)
        case (rsp, _) if rsp.value() =>
          r.handleSuccess(p, false, s"index already exists: '$index'")
        case (_, _) =>
          logger.info(s"creating index $index as it doesn't exist yet with mapping: $map")
          val mapping = new TypeMapping.Builder().properties(map.asJava).build()
          val request = new CreateIndexRequest.Builder().index(index).mappings(mapping).build()

          esClient.indices.create(request).whenCompleteAsync { (rsp: CreateIndexResponse, ex: Throwable) =>
            (rsp, ex) match {
              case (_, ex) if ex != null =>
                r.handleFailure(p, s"ES create mapping: failed due to ${ex.getMessage}", ex)
              case (rsp, _) if rsp.acknowledged() =>
                r.handleSuccess(p, true, s"created index: '$index' and it's mapping: '$mapping'")
              case (rsp, _) =>
                r.handleFailure(p, s"failed to create index: '$index' and it's mapping: '$mapping' with rsp $rsp")
            }
          }
      }
    }
    p.future
  }

  /**
    * Clean Elasticsearch instance by running the following operations:
    *
    * * delete indexes
    * * create mappings
    */
  final def cleanElasticsearch(): Future[Boolean] = {

    for {
      _ <- deleteIndices()
      created <- createElasticsearchMappings()
    } yield created
  }

  /**
    * Delete all indices.
    */
  final def deleteIndices(): Future[Unit] = {

    Future.sequence(indicesToDelete.map { index =>
      val p = Promise[Unit]()
      val existsRequest = new ExistsRequest.Builder().index(index).build()

      esClient.indices().exists(existsRequest).whenCompleteAsync { (rsp: BooleanResponse, ex: Throwable) =>
        (rsp, ex) match {
          case (_, ex) if ex != null =>
            r.handleFailure(p, s"failed checking if index exists $index due to ${ex.getMessage}", ex)
          case (rsp, _) if rsp.value() =>
            logger.info(s"index exists and should become deleted: '$index'")
            esClient.indices()
              .delete { db: DeleteIndexRequest.Builder => db.index(index) }
              .whenCompleteAsync { (rsp: DeleteIndexResponse, ex: Throwable) =>
                (rsp, ex) match {
                  case (rsp, null) if rsp.acknowledged() =>
                    r.handleSuccess(p, (), s"deleted index: '$index'")
                  case (_, ex) =>
                    r.handleFailure(p, s"failed to delete index $index due to ${ex.getMessage}", ex)
                }
              }
          case (_, _) =>
            r.handleSuccess(p, (), s"index doesn't exist and doesn't need to become deleted: '$index'")
        }
      }
      p.future
    }).map(_ => ())
  }

  def closeConnection(): Unit = {
    esClient.shutdown()
  }

}
