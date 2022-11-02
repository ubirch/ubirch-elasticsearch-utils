package com.ubirch.util.elasticsearch.util

import co.elastic.clients.elasticsearch._types.FieldValue
import co.elastic.clients.elasticsearch._types.query_dsl._
import org.joda.time.DateTime

import scala.jdk.CollectionConverters.SeqHasAsJava

object QueryUtil {

  def buildTermQuery(field: String, value: String): Query = {
    new Query.Builder().term(new TermQuery.Builder().field(field).value(value).build()).build()
  }

  def buildTermsQuery(field: String, values: Set[String]): Query = {
    val fieldValues = values.map(FieldValue.of).toSeq.asJava
    val termsQueryFields = new TermsQueryField.Builder().value(fieldValues).build()
    val termsQuery = new TermsQuery.Builder().field(field).terms(termsQueryFields).build()
    new Query.Builder().terms(termsQuery).build()
  }

  def buildDateRangeQuery(fieldName: String, gte: DateTime, lte: DateTime): Query = {
    buildDateRangeQuery(fieldName, Some(gte), Some(lte))
  }

  def buildDateRangeQuery(fieldName: String, gte: Option[DateTime] = None, lte: Option[DateTime] = None): Query = {
    val builder = new RangeQuery.Builder().field(fieldName).format("strict_date_time")
    if (gte.isDefined) builder.from(gte.get.toString)
    if (lte.isDefined) builder.to(lte.get.toString)
    new Query.Builder().range(builder.build()).build()
  }

  /**
    * This method will create a query, that requires the acceptance of the combination of all queries handed
    * over as parameter.
    * @param queries a number of queries that shall be combined
    */
  def buildBoolMustQuery(queries: Seq[Query]): Query = {
    val boolQuery = new BoolQuery.Builder().must(queries.asJava).build
    new Query.Builder().bool(boolQuery).build()
  }

}
