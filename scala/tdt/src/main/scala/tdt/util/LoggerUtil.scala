package com.kunyan.tdt.util

import org.apache.log4j.{BasicConfigurator, Logger, PropertyConfigurator}

/**
  * 写Log操作
  */
object LoggerUtil {

  var logger = Logger.getLogger("TopicDetectionAndTracking")
  BasicConfigurator.configure()
  PropertyConfigurator.configure("/home/mlearning/conf/log4j.properties")

  def exception(e: Exception) = {

    logger.error(e.printStackTrace())

  }

  def error(msg: String): Unit = {

      logger.error(msg)
  }

  def warn(msg: String): Unit = {

      logger.warn(msg)
  }

  def info(msg: String): Unit = {

      logger.info(msg)
  }

  def debug(msg: String): Unit = {

      logger.debug(msg)
  }

}
