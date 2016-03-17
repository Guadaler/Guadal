package com.kunyandata.nlpsuit.util

import java.text.SimpleDateFormat

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
    */
  def get_date(timeStamp: Long, formatString: String): String ={

    var dateFormat = new SimpleDateFormat(formatString)
    //    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val out = dateFormat.format(timeStamp)
    out
  }

}
