package com.kunyan.util

import org.apache.log4j.{PropertyConfigurator, BasicConfigurator, Logger}

/**
  * Created by liumiao on 2016/4/21.
  * 写Log操作
  */
object LoggerUtil {

  var logger = Logger.getLogger("SENTIMENT")
  BasicConfigurator.configure()
  PropertyConfigurator.configure("/home/sentiment/conf/log4j.properties")

  var switch = true

  def exception(e: Exception) = {
    if (switch)
      e.printStackTrace()
    logger.error(e.printStackTrace())

  }

  def error(msg: String): Unit = {
    if (switch)
      logger.error(msg)
  }

  def warn(msg: String): Unit = {
    if (switch)
      logger.warn(msg)
  }

  def info(msg: String): Unit = {
    if (switch)
      logger.info(msg)
  }

  def debug(msg: String): Unit = {
    if (switch)
      logger.debug(msg)
  }

}
