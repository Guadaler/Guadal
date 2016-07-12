package com.kunyan.tdt.process

import com.kunyan.graph.textrank.TextRankBaseOnRdd
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by QQ on 7/5/16.
  */
object StepFour {

  /**
    * 筛选出与事件相关的文章（出现事件词数大于30%的文章）
    *
    * @param communityWords 事件词
    * @param news 所有文章
    * @return 与事件相关的文章
    * @author WangCao
    */
  def filterRelatedNews(communityWords: Array[String],
                        news: RDD[Array[String]]): RDD[Array[String]] = {

    val length = communityWords.length * 0.3

    val tmpNews = news.map(x => {

      var num = 0

      for (i <- communityWords) {

        if (x.contains(i)) {
          num = num + 1
        }

      }

      (x,num)
    })

    var relatedNews =  tmpNews.filter(x => x._2 >= length) .map(x => x._1)

    if (relatedNews.count == 0) {
      relatedNews = tmpNews.filter(x => x._2 >= 1).map(x => x._1)
    }

    relatedNews
  }

  /**
    * 筛选出所有提到股票代码的文章的词集合
    *
    * @param targetNews 所有涉及事件词的文章
    * @return 提到股票的文章的词集合
    * @author WangCao
    */
  def filterStockNews (targetNews: RDD[Array[String]]): Array[String] = {

    val stockNews = targetNews
      .map(line => {

        val tmp = line
          .map(x => x.replaceAll("002[\\d]{3}|000[\\d]{3}|300[\\d]{3}|600[\\d]{3}|60[\\d]{4}", "111111"))

        if (tmp.contains("111111")) {
          line
        } else {
          null
        }

      })
      .filter(line => line != null)

    if (stockNews.count == 0) {
      null
    } else {
      stockNews .map(x => x.mkString(",")).reduce(_ + "," + _).split(",").distinct
    }

  }

  /**
    * 使用textrank提取前10股票词以及其权重
    *
    * @param sc SparkContext
    * @param news 原始新闻
    * @param words 社区中的词
    * @return 股票代码及其权重转化的String格式
    * @author WangCao
    */
  def getStockWeight(sc: SparkContext,
                     news: RDD[Array[String]],
                     words: Array[String]): String = {

    val relatedNews = filterRelatedNews(words, news)
    val wholeText = relatedNews.map(_.mkString(",")).reduce(_ + "," + _).split(",")
    val ranks = TextRankBaseOnRdd.getTopHotWords(sc, wholeText, 5, 10)

    val totalScoreSum = ranks.map(x => x._2).sum

    val ranksMap = ranks.toMap

    val stockNews = filterStockNews(relatedNews)

    val weight = if (stockNews != null) {

      val stockNewsScoreSum = stockNews.length

      val newsWeight = stockNewsScoreSum / totalScoreSum

      val stock = stockNews
        .filter(x => x.matches("002[\\d]{3}|000[\\d]{3}|300[\\d]{3}|600[\\d]{3}|60[\\d]{4}"))

      val stockFinalWeight = stock.distinct.map(x => {

        val score = ranksMap(x)
        val weight = newsWeight * score
        val reduceWeight = f"$weight%1.4f"

        (x, reduceWeight)
      })
        .sortWith(_._2 > _._2).take(10)

      stockFinalWeight.map(x => x._1 + ":" + x._2).distinct.mkString(",")

    } else {
      "No Related Stock be Found"
    }

    weight
  }

}
