package com.ubirch.util.elasticsearch

import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.util.elasticsearch.config.EsHighLevelConfig
import com.ubirch.util.json.Json4sUtil
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.common.xcontent.XContentType
import org.json4s.JValue

object EsBulkClient extends EsBulkClientBase

trait EsBulkClientBase extends StrictLogging {

  private val esClient: RestHighLevelClient = EsHighLevelClient.client

  /**
    * returns current ElasticSearch RestHighLevelClient instance
    *
    * @return esClient as RestHighLevelClient
    */
  def getCurrentEsClient: RestHighLevelClient = esClient

  /**
    * A listener to react after or before the collected bulk of documents is written to the elasticsearch.
    */
  private val listener: BulkProcessor.Listener = new BulkProcessor.Listener() {


    @Override
    def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
      logger.debug(s"beforeBulk($executionId, number of actions: #${request.numberOfActions()}, ${request.estimatedSizeInBytes()})")
    }

    @Override
    def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
      logger.debug(s"afterBulk($executionId, number of actions: #${request.numberOfActions()}, ${request.estimatedSizeInBytes()}) => ${response.getTook}")
      if (response.hasFailures)
        logger.error(s"afterBulk($executionId, some requests of the bulk request failed: ${response.buildFailureMessage}")
    }

    @Override
    def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
      logger.error(s"afterBulk($executionId, number of actions: #${request.numberOfActions()}, ${request.estimatedSizeInBytes()}, trying it once more)", failure)

      bulkAsyncAsJava.accept(request, new ActionListener[BulkResponse]() {

        @Override
        def onResponse(response: BulkResponse): Unit = {
          logger.debug(s"afterBulk; second Trial worked out $response, but has failures: ${response.hasFailures}")
        }

        @Override
        def onFailure(e: Exception): Unit = {
          logger.error(s"after Bulk, second trial failed for request ($request) as well", e)
        }
      })
    }

  }

  private val bulkAsyncAsJava: BiConsumer[BulkRequest, ActionListener[BulkResponse]] =
    new BiConsumer[BulkRequest, ActionListener[BulkResponse]] {
      override def accept(bulkRequest: BulkRequest, actionListener: ActionListener[BulkResponse]): Unit = {
        esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, actionListener)
      }
    }

  /**
    * Bulkprocessor with it's different configurations regarding the time of storage.
    */
  private val bulkProcessor: BulkProcessor = BulkProcessor.builder(bulkAsyncAsJava, listener)
    .setBulkActions(EsHighLevelConfig.bulkActions)
    .setBulkSize(new ByteSizeValue(EsHighLevelConfig.bulkSize, ByteSizeUnit.MB))
    .setFlushInterval(TimeValue.timeValueSeconds(EsHighLevelConfig.flushInterval))
    .setConcurrentRequests(EsHighLevelConfig.concurrentRequests)
    .setBackoffPolicy(
      BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), 10))
    .build()

  /**
    * Method that stores a document to the bulkprocessor that by it's configuration
    * stores all received documents after a certain amount or time to the elasticsearch.
    *
    * @param docIndex index to store the document to
    * @param docId    id of the new document
    * @param doc      document itself
    */
  def storeDocBulk(docIndex: String,
                   docId: String,
                   doc: JValue
                  ): Boolean = {
    try {
      bulkProcessor.add(
        new IndexRequest(docIndex).id(docId)
          .source(Json4sUtil.jvalue2String(doc), XContentType.JSON)
      )
      true
    } catch {
      case ex: Throwable =>
        logger.debug(s"ES error - storeDocBulk(): docIndex=$docIndex docId=$docId doc=$doc ", ex)
        false
    }
  }

  def closeConnection(): Unit = {
    bulkProcessor.close()
    bulkProcessor.awaitClose(30L, TimeUnit.SECONDS)
  }

  sys.addShutdownHook(closeConnection())


}
