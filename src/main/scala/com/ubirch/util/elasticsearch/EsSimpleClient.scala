package com.ubirch.util.elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import co.elastic.clients.elasticsearch._types.query_dsl.{MatchAllQuery, Query}
import co.elastic.clients.elasticsearch._types.{Refresh, SortOptions}
import co.elastic.clients.elasticsearch.core._
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.{Logger, StrictLogging}
import com.ubirch.util.deepCheck.model.DeepCheckResponse
import com.ubirch.util.elasticsearch.config.EsAsyncClientConfig
import com.ubirch.util.elasticsearch.util.{ESException, QueryUtil, ResultUtil}
import com.ubirch.util.json.{Json4sUtil, JsonFormats}
import com.ubirch.util.uuid.UUIDUtil
import monix.execution.FutureUtils
import monix.execution.Scheduler.Implicits.global
import org.json4s.jackson.JsonMethods.fromJsonNode
import org.json4s.{Formats, JValue}

import java.io.{IOException, StringReader}
import java.net.{ConnectException, SocketTimeoutException}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.jdk.CollectionConverters._

object EsSimpleClient extends EsSimpleClientBase

/**
  * This is an abstraction for the elasticsearch Higher Level Client
  */
trait EsSimpleClientBase extends StrictLogging {

  implicit val formats: Formats = JsonFormats.default
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val log: Logger = logger
  private[elasticsearch] val r = new ResultUtil()
  private[elasticsearch] val esClient: ElasticsearchAsyncClient = EsAsyncClient.asyncClient
  private[elasticsearch] val maxRetries: Int = EsAsyncClientConfig.maxRetries
  private val retryDelay: Int = EsAsyncClientConfig.retryDelayFactor

  /**
    * This method stores a document to the index.
    *
    * @param docIndex          name of the index into which the current document should be stored
    * @param docIdOpt          unique id which identifies current document uniquely inside the index
    * @param doc               document as a JValue which should be stored
    * @param retry             defines how of the request has been repeated in case of an error
    * @param waitingForRefresh defines, if the request has to wait until the index has become refreshed (default config
    *                          mostly 1 second => prevents inconsistent data if the deleted doc is queried quickly after
    *                          deletion)
    * @return Boolean indicating success
    */
  def storeDoc(
                docIndex: String,
                doc: JValue,
                docIdOpt: Option[String] = None,
                retry: Int = 0,
                waitingForRefresh: Boolean = false): Future[Unit] = {

    val p = Promise[Unit]()
    try {
      val docId = docIdOpt.getOrElse(UUIDUtil.uuidStr)
      Json4sUtil.jvalue2String(doc) match {
        case docStr if docStr.nonEmpty =>
          val request = new IndexRequest.Builder().index(docIndex).withJson(new StringReader(docStr)).id(docId)
          if (waitingForRefresh) request.refresh(Refresh.WaitFor)

          esClient.index(request.build()).whenCompleteAsync { (rsp: IndexResponse, ex: Throwable) =>
            (rsp, ex) match {
              case (rsp, null) => r.handleSuccess(p, (), s"stored document with id $docId $doc and rsp ${rsp.result()}")
              case (_, ex) => r.handleFailure(p, s"failed to store document $doc with ex ${ex.getMessage}", ex)
            }
          }
        case _ => r.handleFailure(p, s"parsing JValue to string of ($doc) failed")
      }
    } catch {
      case ex: Throwable => r.handleFailure(p, s"storeDoc failed due to unexpected error, ${ex.getMessage}", ex)
    }
    p.future
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException | _: TimeoutException | _: ESException) =>
      if (retry < maxRetries) {
        logger.error(
          s"ES error storeDoc() failed; will try again #${retry + 1}: index=$docIndex doc=$doc  id=$docIdOpt",
          ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          storeDoc(docIndex, doc, docIdOpt, retry + 1, waitingForRefresh)
        }.flatMap(future => future)
      } else {
        logger.error(
          s"ES error storeDoc() failed; no (more) retries ($retry/$maxRetries): index=$docIndex doc=$doc  id=$docIdOpt",
          ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(
        s"ES error storeDoc() failed; won't try again, due to unknown error type: index=$docIndex doc=$doc  id=$docIdOpt",
        ex)
      Future.failed(ex)
  }
  /**
    * This method returns a document by it's id.
    *
    * @param docIndex name of the ElasticSearch index
    * @param docId    unique Id per Document
    * @param retry    defines how of the request has been repeated in case of an error
    */
  def getDoc(docIndex: String, docId: String, retry: Int = 0): Future[Option[JValue]] = {

    val p = Promise[Option[JValue]]()

    try {
      val request = new GetRequest.Builder().index(docIndex).id(docId).build()
      esClient.get(request, classOf[ObjectNode]).whenCompleteAsync { (rsp: GetResponse[ObjectNode], ex: Throwable) =>
        (rsp, ex) match {
          case (_, ex) if ex != null =>
            r.handleFailure(p, s"getDoc for id $docId failed with ex ${ex.getMessage}", ex)
          case (rsp, _) if rsp.found() =>
            r.handleSuccess(p, Some(fromJsonNode(rsp.source())), s"found document with id $docId and rsp $rsp")
          case (rsp, _) =>
            r.handleSuccess(p, None, s"no document was found for the id: $docId with response $rsp")
        }
      }
    } catch {
      case ex: Throwable => r.handleFailure(p, s"getDoc failed due to unexpected error, ${ex.getMessage}", ex)
    }
    p.future
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException | _: TimeoutException | _: ESException) =>
      if (retry < maxRetries) {
        logger.warn(
          s"Es error getDoc() failed; will try again #${retry + 1}: retrieving document with id $docId from index=$docIndex",
          ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          getDoc(docIndex, docId, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(
          s"Es error getDoc() failed; no (more) retries ($retry/$maxRetries): retrieving document with id $docId from index=$docIndex",
          ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(
        s"Es error getDoc() failed; won't try again, due to unknown error type: retrieving document with id $docId from index=$docIndex",
        ex)
      Future.failed(ex)
  }

  /**
    * This method returns all documents queried and sorted if wished for.
    *
    * @param docIndex name of the ElasticSearch index
    * @param query    search query
    * @param from     pagination from (may be 0 or larger)
    * @param size     maximum number of results (may be 0 or larger)
    * @param sort     optional result sort
    * @param retry    defines how of the request has been repeated in case of an error
    * @return
    */
  def getDocs(
               docIndex: String,
               query: Option[Query] = None,
               from: Int = 0,
               size: Int = 10,
               sort: Seq[SortOptions] = Seq.empty,
               retry: Int = 0): Future[Seq[JValue]] = {

    val p = Promise[Seq[JValue]]()
    try {
      if (from < 0 || size < 0) r.handleFailure(p, s"from ($from) and size ($size)may not be negative")
      else {
        val finalQuery = query.getOrElse(new MatchAllQuery.Builder().build()._toQuery())
        val request = new SearchRequest.Builder().index(docIndex).query(finalQuery)
        request.from(from)
        request.size(size)
        request.sort(sort.asJava)
        esClient.search(request.build(), classOf[ObjectNode]).whenCompleteAsync {
          (rsp: SearchResponse[ObjectNode], ex: Throwable) =>
            (rsp, ex) match {
              case (rsp, null) =>
                val result = rsp.hits().hits().asScala.toList.map(on => fromJsonNode(on.source()))
                r.handleSuccess(p, result, s"getDocs; ${result.size} documents were found")
              case (_, ex) =>
                r.handleFailure(p, s"getDocs failed with query $finalQuery due to ${ex.getMessage}", ex)
            }
        }
      }
    } catch {
      case ex: Throwable => r.handleFailure(p, s"getDocs failed due to unexpected error, ${ex.getMessage}", ex)
    }
    p.future
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException | _: TimeoutException | _: ESException) =>
      if (retry < maxRetries) {
        logger.warn(
          s"ES error getDocs() failed; will try again #${retry + 1}: index=$docIndex query=$query from=$from size=$size sort=$sort",
          ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          getDocs(docIndex, query, from, size, sort, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(
          s"ES error getDocs() failed; no (more) retries ($retry/$maxRetries): index=$docIndex query=$query from=$from size=$size sort=$sort",
          ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(
        s"ES error getDocs() failed; won't try again, due to unknown error type: index=$docIndex query=$query from=$from size=$size sort=$sort",
        ex)
      Future.failed(ex)
  }

  /**
    * This method removes a document by it's id from the index.
    *
    * @param docIndex          name of the index
    * @param field             field of term query for deletion
    * @param value             value of term query for deletion
    * @param retry             defines how of the request has been repeated in case of an error
    * @param waitingForRefresh defines, if the request has to wait until the index has become refreshed (default config
    *                          mostly 1 second => prevents inconsistent data if the deleted doc is queried quickly after
    *                          deletion)
    * @return
    */
  def deleteDoc(
                 docIndex: String,
                 field: String,
                 value: String,
                 retry: Int = 0,
                 waitingForRefresh: Boolean = false): Future[Unit] = {

    val p = Promise[Unit]()
    try {
      val query = QueryUtil.buildTermQuery(field, value)
      val request = new DeleteByQueryRequest.Builder().index(docIndex).query(query)
      if (waitingForRefresh) request.refresh(true)
      esClient.deleteByQuery(request.build()).whenCompleteAsync { (rsp: DeleteByQueryResponse, ex: Throwable) =>
        (rsp, ex) match {
          case (response, null) if response.deleted() == 1 =>
            r.handleSuccess(p, (), s"deleted doc $docIndex $field $value")
          case (_, null) =>
            r.handleSuccess(p, (), s"didn't delete doc as it didn't exist anymore $docIndex $field $value")
          case (_, ex) =>
            r.handleFailure(p, s"failed to delete $value due to ${ex.getMessage}", ex)
        }
      }
    } catch {
      case ex: Throwable =>
        r.handleFailure(p, s"deleteDoc failed due to unexpected error, ${ex.getMessage}", ex)
    }
    p.future
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException | _: TimeoutException | _: ESException) =>
      if (retry < maxRetries) {
        logger.warn(
          s"ES error deleteDoc() failed; will try again #${retry + 1}: index=$docIndex field=$field value=$value",
          ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          deleteDoc(docIndex, field, value, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(
          s"ES error deleteDoc() failed; no (more) retries ($retry/$maxRetries): index=$docIndex field=$field value=$value",
          ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(
        s"ES error deleteDoc() failed; won't try again, due to unknown error type: index=$docIndex field=$field value=$value",
        ex)
      Future.failed(ex)
  }
  /**
    * Query an index for a single record to test connectivity to Elasticsearch.
    *
    * @param docIndex index to query
    * @return result of connectivity check
    */
  def connectivityCheck(docIndex: String = "foo"): Future[DeepCheckResponse] =
    getDocs(docIndex = docIndex, size = 1)
      .map(_ => DeepCheckResponse())
      .recover {
        case ex =>
          logger.error(s"ES error connectivityCheck() failed; deepcheck failing index=$docIndex", ex)
          DeepCheckResponse(status = false, messages = Seq(ex.getMessage))
      }

  def closeConnection(): Unit = {
    esClient.shutdown()
  }

}
