package com.kunyandata.nlpsuit.util

import scala.collection.mutable.ListBuffer

/**
  * Created by yang on 5/31/16.
  */
object TextUtil {

  /**
    * 将文章根据逗号或句号分割成若干字符串
    */
  def splitArticle(article: String): ListBuffer[String] = {

    val list = ListBuffer[String]()
    val articleLength = article.length
    var beginIndex = 0
    var endIndex = 2999

    while (beginIndex < articleLength) {

      endIndex = beginIndex + 2999

      if (endIndex + 1 > articleLength) {
        list += article.substring(beginIndex)
        return list
      }

      var content = article.substring(beginIndex, endIndex + 1)
      val commaZhIndex = content.lastIndexOf('，')
      val commaEnIndex = content.lastIndexOf(',')
      val periodZhIndex = content.lastIndexOf('。')
      val periodEnIndex = content.lastIndexOf('.')

      endIndex = max(commaEnIndex, commaZhIndex, periodEnIndex, periodZhIndex) + beginIndex
      content = article.substring(beginIndex, endIndex + 1)
      list += content
      beginIndex = endIndex + 1

    }

    list
  }

  def max(indexes: Int*): Int = {

    var max = 0
    indexes.foreach(index => {
      if (index >  max && index > 0) {
        max = index
      }
    })

    max
  }

}
