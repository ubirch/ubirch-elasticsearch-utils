package com.ubirch.util.elasticsearch.util

import com.typesafe.scalalogging.Logger

import scala.concurrent.Promise

class ResultUtil {

  def handleSuccess[T](p: Promise[T], r: T, msg: String)(implicit logger: Logger): Promise[T] = {
    logger.info("ES success, " + msg)
    p.success(r)
  }

  def handleFailure[T](p: Promise[T], errorMsg: String)(implicit logger: Logger): Promise[T] = {
    logger.error("ES error, " + errorMsg)
    p.failure(ESUtilException("ES error, " + errorMsg))
  }

  def handleFailure[T](p: Promise[T], errorMsg: String, ex: Throwable)(implicit logger: Logger): Promise[T] = {
    logger.error("ES error, " + errorMsg)
    p.failure(ESUtilExceptionV("ES error, " + errorMsg, ex))
  }

}
