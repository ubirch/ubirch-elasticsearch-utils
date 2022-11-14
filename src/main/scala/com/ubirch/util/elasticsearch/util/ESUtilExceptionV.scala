package com.ubirch.util.elasticsearch.util

trait ESException

case class ESUtilException(msg: String) extends Exception(msg) with ESException

//verbose version
case class ESUtilExceptionV(msg: String, ex: Throwable) extends Exception(msg, ex) with ESException
