package com.kunyan.nlpsuit.classification

import scala.collection.{mutable, Map}

/**
  * Created by QQ on 2016/2/18.
  * 基于规则的标题分类
  */
object Regular {

  /**
    * 规则分类过程
    * @param textString 标题字符串
    * @param categoryKeywords 类别字典，（股票、行业或者概念板块）
    * @param categoryList 所属类别List
    */
  def grep(textString: String, categoryKeywords: Map[String, Array[String]],
                   categoryList: mutable.MutableList[String]): Unit = {

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

    val industryList = new mutable.MutableList[String]
    val stockList = new mutable.MutableList[String]
    val sectionList = new mutable.MutableList[String]

    //    行业分类
    grep(textString, indusDict, industryList)
    //    概念板块分类
    grep(textString, sectDict, sectionList)
    //    股票分类
    grep(textString, stockDict, stockList)

    industryList.mkString(",")
    sectionList.mkString(",")
    stockList.mkString(",")

    //    返回值的顺序为股票，行业，版块
    (stockList.mkString(","), industryList.mkString(","), sectionList.mkString(","))
  }
}
