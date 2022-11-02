package com.ubirch.util.elasticsearch.util

import co.elastic.clients.elasticsearch._types.{FieldSort, SortOptions, SortOrder}

object SortUtil {

  def buildSortOptions(field: String, asc: Boolean = true): SortOptions = {
    val order = if (asc) SortOrder.Asc else SortOrder.Desc
    new SortOptions.Builder().field(new FieldSort.Builder().field(field).order(order).build()).build()
  }

}
