package com.kunyan.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by QQ on 2016/2/23.
  * 时间工具
  */
object TimeUtil {

  /**
    * 根据时间戳获取时间字符串
    * @param timeStamp 时间戳
    * @param formatString 时间字符串格式
    * @return 返回值为格式化后的时间字符串
    * @author QQ
    */
  def get_date(timeStamp: Long, formatString: String): String = {

    val dateFormat = new SimpleDateFormat(formatString)
    val out = dateFormat.format(timeStamp)

    out
  }

  /**
    * 获取时间
    * @param formatString 时间格式
    * @return 返回时间字符串
    * @author liumiao
    */
  def get_date(formatString: String): String = {

    val now = new Date()
    val dateFormat = new SimpleDateFormat(formatString)
    val out = dateFormat.format(now)

    out
  }

}
