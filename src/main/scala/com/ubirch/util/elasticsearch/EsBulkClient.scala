package com.ubirch.util.elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import co.elastic.clients.elasticsearch._types.Refresh
import co.elastic.clients.elasticsearch.core.bulk.{ BulkOperation, IndexOperation }
import co.elastic.clients.elasticsearch.core.{ BulkRequest, BulkResponse }
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.{ Logger, StrictLogging }
import com.ubirch.util.elasticsearch.util.ResultUtil
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.asJsonNode

import scala.jdk.CollectionConverters.CollectionHasAsScala

import scala.concurrent.{ Future, Promise }

object EsBulkClient extends EsBulkClientBase

trait EsBulkClientBase extends StrictLogging {

  private[elasticsearch] val esClient: ElasticsearchAsyncClient = EsAsyncClient.asyncClient
  implicit val log: Logger = logger
  private[elasticsearch] val r = new ResultUtil()

  /**
    * returns current ElasticSearch RestHighLevelClient instance
    *
    * @return esClient as RestHighLevelClient
    */
  def getCurrentEsClient: ElasticsearchAsyncClient = esClient

  def storeDocBulk(
    docIndex: String,
    idsAndDocs: Map[String, JValue],
    waitingForRefresh: Boolean = false): Future[Unit] = {

    val p = Promise[Unit]()
    try {
      val br = new BulkRequest.Builder()
      idsAndDocs.map {
        case (id, doc) =>
          val indexOp =
            new IndexOperation.Builder[ObjectNode]().index(docIndex).id(id).document(asJsonNode(doc).deepCopy()).build()
          val bulkOp = new BulkOperation.Builder().index(indexOp).build()
          br.operations(bulkOp)
      }
      if (waitingForRefresh) br.refresh(Refresh.WaitFor)

      esClient.bulk(br.build()).whenCompleteAsync { (rsp: BulkResponse, ex: Throwable) =>
        (rsp, ex) match {
          case (_, ex) if ex != null =>
            r.handleFailure(p, s"storeDocBulk failed to insert bulk of ${idsAndDocs.size} docs", ex)
          case (rsp, _) if rsp.errors() =>
            val errors = rsp.items().asScala.map(_.error()).mkString("; ")
            r.handleFailure(p, s"storeDocBulk failed with errors $errors to insert ${idsAndDocs.size} docs.")
          case (rsp, _) =>
            r.handleSuccess(p, (), s"storeDocBulk succeeded to insert ${idsAndDocs.size} docs; took ${rsp.took()} ms.")
        }
      }
    } catch {
      case ex: Throwable => r.handleFailure(p, s"deleteDoc failed due to unexpected error, ${ex.getMessage}", ex)
    }
    p.future
  }

  def closeConnection(): Unit = {
    esClient.shutdown()
  }

  sys.addShutdownHook(closeConnection())
}
