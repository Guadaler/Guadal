package com.kunyandata.nlpsuit.classification

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}

/**
  * Created by QQ on 2016/2/18.
  * 基于规则的标题分类
  */
object Regular {

  /**
    * 股票和概念板块的分类，基于词典。
    *
    * @param wordSegNoStop 分词后的文本
    * @param categoryKeywords 类别词典，主要是股票和板块词典
    * @return 板块或者行业名称组成的字符串,逗号分割
    */
  def grep(wordSegNoStop: Array[String], categoryKeywords: Map[String, Array[String]]): String = {

    val categoryList = ArrayBuffer[String]()

    for (cate: String <- categoryKeywords.keys) {

      var i_control = true

      for (keyword: String <- categoryKeywords(cate) if i_control) {

        val exists = wordSegNoStop.contains(keyword)

        if (exists) {

          categoryList.append(cate)
          i_control = false

        }

      }

    }

    categoryList.mkString(",")

  }

  /**
    * 规则分类过程
    * @param textString 标题字符串
    * @param categoryKeywords 类别字典，（股票、行业或者概念板块）
    * @return 板块或者行业名称组成的字符串,逗号分割
    */
  private def grep(textString: String, categoryKeywords: Map[String, Array[String]]): String = {

    val categoryList = ArrayBuffer[String]()

    for (indus: String <- categoryKeywords.keys) {

      var i_control = true

      for (keyword: String <- categoryKeywords(indus) if i_control) {

        val exists = textString.contains(keyword)

        if (exists) {

          categoryList.+=(indus)
          i_control = false

        }

      }

    }

    categoryList.mkString(",")

  }

  /**
    * 基于规则的分类
    * @param textString 输入的文本标题字符串
    * @param stockDict 股票分类词典
    * @param indusDict 行业分类词典
    * @param sectDict 板块分类词典
    * @return 返回（股票: String，行业: String，版块: String）
    */
  def predict(textString: String, stockDict: Map[String, Array[String]],
              indusDict: Map[String, Array[String]],
              sectDict: Map[String, Array[String]]) = {

    //    行业分类
    val industryList = grep(textString, indusDict)
    //    概念板块分类
    val sectionList = grep(textString, sectDict)
    //    股票分类
    val stockList = grep(textString, stockDict)

    //    返回值的顺序为股票，行业，版块
    (stockList.mkString(","), industryList.mkString(","), sectionList.mkString(","))

  }
}
