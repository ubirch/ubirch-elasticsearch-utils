package com.ubirch.util.elasticsearch

import java.io.IOException
import java.net.{ConnectException, SocketTimeoutException}

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.deepCheck.model.DeepCheckResponse
import com.ubirch.util.elasticsearch.config.EsHighLevelConfig
import com.ubirch.util.json.{Json4sUtil, JsonFormats}
import com.ubirch.util.uuid.UUIDUtil
import monix.execution.FutureUtils
import monix.execution.Scheduler.Implicits.global
import org.elasticsearch.action.DocWriteResponse.Result
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.{ActionListener, DocWriteResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.aggregations.metrics.{Avg, AvgAggregationBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilder
import org.json4s.{Formats, JValue}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}

object EsSimpleClient extends EsSimpleClientBase

/**
  * This is an abstraction for the elasticsearch Higher Level Client
  */
trait EsSimpleClientBase extends StrictLogging {

  implicit val formats: Formats = JsonFormats.default
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  private val esClient: RestHighLevelClient = EsHighLevelClient.client
  private val maxRetries: Int = EsHighLevelConfig.maxRetries
  private val retryDelay: Int = EsHighLevelConfig.retryDelayFactor

  /**
    * returns current ElasticSearch Transport Client instance
    *
    * @return esClient as TransportClient
    */
  def getCurrentEsClient: RestHighLevelClient = esClient

  /**
    * This method stores a document to the index.
    *
    * @param docIndex name of the index into which the current document should be stored
    * @param docIdOpt unique id which identifies current document uniquely inside the index
    * @param doc      document as a JValue which should be stored
    * @return Boolean indicating success
    */
  @throws[Throwable]
  def storeDoc(docIndex: String,
               doc: JValue,
               docIdOpt: Option[String] = None,
               retry: Int = 0): Future[Boolean] = {

    val docId = docIdOpt.getOrElse(UUIDUtil.uuidStr)

    Json4sUtil.jvalue2String(doc) match {

      case docStr if docStr.nonEmpty =>

        val request = new IndexRequest(docIndex).id(docId).source(docStr, XContentType.JSON)

        val promise = Promise[IndexResponse]()

        esClient.indexAsync(request, RequestOptions.DEFAULT, createActionListener(promise))

        promise.future.map { response =>

          val result = response.getResult

          if (result == Result.CREATED || result == Result.UPDATED) {
            logger.debug(s"the document was successfully $result with id $docId $doc")
            true
          } else throw new Exception(s"storing of document $doc failed with result $result and response $response")
        }

      case _ => throw new Exception(s"JValue parsing to string of ($doc) failed ")

    }
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException) =>
      if (retry < maxRetries) {
        logger.error(s"ES error storeDoc(): index=$docIndex doc=$doc  id=$docIdOpt failed; will try again #${retry + 1}", ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          storeDoc(docIndex, doc, docIdOpt, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(s"ES error storeDoc(): index=$docIndex doc=$doc  id=$docIdOpt failed, no retry ($retry/$maxRetries)  ", ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(s"ES error storeDoc(): index=$docIndex doc=$doc  id=$docIdOpt failed; won't try again, due to unknown error type", ex)
      Future.failed(ex)
  }


  /**
    * This method returns a document by it's id.
    *
    * @param docIndex name of the ElasticSearch index
    * @param docId    unique Id per Document
    */
  @throws[Throwable]
  def getDoc(docIndex: String,
             docId: String,
             retry: Int = 0): Future[Option[JValue]] = {

    val search = new SearchSourceBuilder().query(QueryBuilders.idsQuery.addIds(docId))
    val request = new SearchRequest(docIndex).source(search)

    val promise = Promise[SearchResponse]()
    esClient.searchAsync(request, RequestOptions.DEFAULT, createActionListener[SearchResponse](promise))

    promise.future.map {

      case response if response.getHits.getTotalHits.value == 1 =>
        response.getHits.getHits.map { hit =>
          Json4sUtil.string2JValue(hit.getSourceAsString)
        }.filter(_.isDefined).map(_.get.extract[JValue]).headOption

      case response if response.getHits.getTotalHits.value > 0 =>
        logger.warn(s"ES confusion, found more than one document for the id: $docId")
        None

      case response =>
        logger.info(s"no document was found for the id: $docId with response $response")
        None

    }
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException) =>
      if (retry < maxRetries) {
        logger.error(s"Es error getDoc(): retrieving document with id $docId from index=$docIndex failed; will try again #${retry + 1}", ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          getDoc(docIndex, docId, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(s"Es error getDoc(): retrieving document with id $docId from index=$docIndex failed, no retry ($retry/$maxRetries)  ", ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(s"Es error getDoc(): retrieving document with id $docId from index=$docIndex failed; won't try again, due to unknown error type", ex)
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
    * @return
    */
  @throws[Throwable]
  def getDocs(docIndex: String,
              query: Option[QueryBuilder] = None,
              from: Option[Int] = None,
              size: Option[Int] = None,
              sort: Option[SortBuilder[_]] = None,
              retry: Int = 0): Future[List[JValue]] = {

    val search = new SearchSourceBuilder()
    if (query.isDefined) search.query(query.get)
    if (from.isDefined) search.from(from.get)
    if (size.isDefined) search.size(size.get)
    if (sort.isDefined) search.sort(sort.get)

    val request = new SearchRequest(docIndex).source(search)

    val promise = Promise[SearchResponse]()
    esClient.searchAsync(request, RequestOptions.DEFAULT, createActionListener[SearchResponse](promise))

    promise.future.map {

      case response if response.getHits.getTotalHits.value > 0 =>
        response.getHits.getHits.map { hit =>
          Json4sUtil.string2JValue(hit.getSourceAsString)
        }.filter(_.isDefined).map(_.get.extract[JValue]).toList

      case _ =>
        List()
    }
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException) =>
      if (retry < maxRetries) {
        logger.error(s"ES error getDocs(): index=$docIndex query=$query from=$from size=$size sort=$sort failed; will try again #${retry + 1}", ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          getDocs(docIndex, query, from, size, sort, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(s"ES error getDocs(): index=$docIndex query=$query from=$from size=$size sort=$sort failed, no retry ($retry/$maxRetries)  ", ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(s"ES error getDocs(): index=$docIndex query=$query from=$from size=$size sort=$sort failed; won't try again, due to unknown error type", ex)
      Future.failed(ex)
  }


  /**
    * This method queries an average aggregation and returns a double.
    *
    * @param docIndex name of the ElasticSearch index
    * @param query    search query
    * @param avgAgg   average function
    * @return Option[Double]
    */
  @throws[Throwable]
  def getAverage(docIndex: String,
                 query: Option[QueryBuilder] = None,
                 avgAgg: AvgAggregationBuilder,
                 retry: Int = 0): Future[Option[Double]] = {

    val search = new SearchSourceBuilder().aggregation(avgAgg)
    if (query.isDefined) search.query(query.get)
    val request = new SearchRequest(docIndex).source(search)

    val promise = Promise[SearchResponse]
    esClient.searchAsync(request, RequestOptions.DEFAULT, createActionListener[SearchResponse](promise))

    promise.future.map {

      case response if response.getHits.getTotalHits.value > 0 =>
        val agg = response.getAggregations
        val avg: Avg = agg.get(avgAgg.getName)
        avg.getValue match {

          case avgValue if avgValue.isInfinity =>
            None
          case avgValue if !avgValue.equals(Double.NaN) =>
            Some(avgValue)
          case _ =>
            None
        }
      case _ =>
        None
    }
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException) =>
      if (retry < maxRetries) {
        logger.error(s"ES error getAverage(): index=$docIndex query=$query avgAgg=$avgAgg failed; will try again #${retry + 1}", ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          getAverage(docIndex, query, avgAgg, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(s"ES error getAverage(): index=$docIndex query=$query avgAgg=$avgAgg failed, no retry ($retry/$maxRetries)  ", ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(s"ES error getAverage(): index=$docIndex query=$query avgAgg=$avgAgg failed; won't try again, due to unknown error type ", ex)
      Future.failed(ex)
  }

  /**
    * This method removes a document by it's id from the index.
    *
    * @param docIndex name of the index
    * @param docId    unique id
    * @return
    */
  @throws[Throwable]
  def deleteDoc(docIndex: String, docId: String, retry: Int = 0): Future[Boolean] = {

    val request = new DeleteRequest(docIndex, docId)

    val promise = Promise[DeleteResponse]()
    esClient.deleteAsync(request, RequestOptions.DEFAULT, createActionListener[DeleteResponse](promise))
    promise.future.map {
      case response if response.getResult == DocWriteResponse.Result.NOT_FOUND =>
        false
      case response if response.getResult == DocWriteResponse.Result.DELETED =>
        true
      case response =>
        throw new IOException(s"ES error, unexpected response $response")

    }
  }.recoverWith {
    case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException) =>
      if (retry < maxRetries) {
        logger.error(s"ES error deleteDoc(): index=$docIndex docId=$docId failed; will try again #${retry + 1}", ex)
        FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
          deleteDoc(docIndex, docId, retry + 1)
        }.flatMap(future => future)
      } else {
        logger.error(s"ES error deleteDoc(): index=$docIndex docId=$docId failed, no retry ($retry/$maxRetries)  ", ex)
        Future.failed(ex)
      }

    case ex: Throwable =>
      logger.error(s"ES error deleteDoc(): index=$docIndex docId=$docId failed; won't try again, due to unknown error type", ex)
      Future.failed(ex)
  }

  /**
    * Query an index for a single record to test connectivity to Elasticsearch.
    *
    * @param docIndex index to query
    * @return result of connectivity check
    */
  def connectivityCheck(docIndex: String = "foo", retry: Int = 0): Future[DeepCheckResponse] =

    getDocs(docIndex = docIndex, size = Some(1))
      .map(_ => DeepCheckResponse())
      .recoverWith {
        case ex@(_: IOException | _: ConnectException | _: SocketTimeoutException) =>
          if (retry < maxRetries) {
            logger.error(s"ES error connectivyCheck(): deepcheck failing index=$docIndex failed; will try again #${retry + 1}", ex)
            FutureUtils.delayedResult((retry + 1) * retryDelay.seconds) {
              connectivityCheck(docIndex, retry + 1)
            }.flatMap(future => future)
          } else {
            logger.error(s"ES error connectivyCheck(): deepcheck failing index=$docIndex failed, no retry ($retry/$maxRetries)  ", ex)
            Future.successful(DeepCheckResponse(
              status = false,
              messages = Seq(ex.getMessage)
            ))
          }

        case ex: Throwable =>
          logger.error(s"ES error connectivyCheck(): deepcheck failing index=$docIndex failed; won't try again, due to unknown error type", ex)
          Future.failed(ex)
      }


  /**
    * Helper method to create an actionListnener.
    */
  private def createActionListener[T](promise: Promise[T]): ActionListener[T] = {

    new ActionListener[T] {

      override def onResponse(response: T): Unit = promise.success(response)

      override def onFailure(e: Exception): Unit = promise.failure(e)
    }
  }

  def closeConnection(): Unit = {
    esClient.close()
  }


}
