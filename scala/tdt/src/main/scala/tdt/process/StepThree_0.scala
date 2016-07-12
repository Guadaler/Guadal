package com.kunyan.tdt

import java.io.PrintWriter

import com.kunyan.graph.textrank.TextRankBaseOnRdd
import com.kunyan.tdt.subjectWord.{SubjectWord2, SubjectWord}
import com.kunyan.tdt.util.{HBaseUtil, LoggerUtil}
import com.kunyandata.nlpsuit.classification.{Bayes, Regular}
import com.kunyandata.nlpsuit.util.{JsonConfig, KunyanConf, TextPreprocessing}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by CC on 2016/6/28.
  * 利用textrank进行社区关键词提取
  */
object StepThree_0 {

//  /**
//    * 筛选出出现了社区内词的所有文章
//    * 不过滤，包含即取
//    * @param communityWords 聚类后社区中的词
//    * @param textWords 每篇新闻（分完词）
//    * @return 新闻中有词的则返回true
//    * @author QQ
//    */
//  def filterFunc_1(communityWords: Array[String],
//                 textWords: Array[String]): Boolean = {
//    communityWords.foreach(word => {
//
//      if (textWords.contains(word)) {
//        return true
//      }
//
//    })
//
//    false
//  }

//  /**
//    * 筛选出出现了社区内词的所有文章
//    * 阈值过滤
//    * @param communityWords 聚类后社区中的词
//    * @param textWords 每篇新闻（分完词）
//    * @return 新闻中有词的则返回true
//    */
//  def filterFunc_2(communityWords: Array[String],
//                 textWords: Array[String]): Boolean = {
//    communityWords.foreach(word => {
//
//      if (textWords.count(line => line.equals(word)) > 5){
//        return true
//      }
//
//    })
//
//    false
//  }

  /**
    * 筛选出出现了社区内词的所有文章
    * 阈值+同现度:当一篇文章至少满足两个条件：
    * ① 至少有一个事件词的出现次数大于3
    * ② 文中至少出现了三个事件词
    * @param communityWords 社区词
    * @param textWords 单篇文章
    * @return 返回判断结果
    */
  def filterFunc_3(communityWords: Array[String],
                 textWords: Array[String]): Boolean = {

    var aFlag=0  //同现度, 每出现一个事件词就 +1
    var bFlag=0  //当单个关键词阈值>3, bFlag +1

    communityWords.foreach(word => {

      if(textWords.contains(word)) {
        aFlag += 1
      }

      if (textWords.count(line => line.equals(word)) > 3){

        aFlag += 1
        bFlag += 1

      }

    })

    if(aFlag >2 && bFlag>2) {
      return true
    }

    false
  }

  /**
    * 使用textrank对每个社区中的词提取关键词
    *
    * @param sc SparkContext
    * @param news 原始新闻
    * @param communityWords 社区中的词
    * @return 关键词
    */
  def getKeyWord(sc: SparkContext,
                 news: RDD[Array[String]],
                 communityWords: Array[String],
                 num: Int,
                 count:Int): Array[String] = {

    val relatedNews = news.filter(filterFunc_3(communityWords, _))  //找出包含事件词的所有文章

    //设置阈值之后，如果包含事件词的文章为0
    if(relatedNews.count()>0){

      val wholeText = relatedNews.map(_.mkString(",")).reduce(_ + "," + _).split(",")

      val ranks = TextRankBaseOnRdd.getTopHotWords(sc, wholeText, 5, 10)  //TextRanck

      ranks.filter(line => communityWords.contains(line._1)).map(x => x._1).take(num)

    }else{
      Array("kong")
    }

  }

  /**
    * 抽取所有社区的主题词
    * @param sc SparkContext
    * @param news 新闻集合
    * @param community 社区中的词
    * @param num 取词个数
    * @return
    */
  def getKeyWordsForTotalCommunity(sc: SparkContext,
                                   news: RDD[Array[String]],
                                   community: RDD[(String, Array[String])],
                                   num: Int): Array[(String, String)] = {
    var count=0

    val keyWords = community.collect.map(x => {

        count +=1
        (x._1, getKeyWord(sc, news, x._2.toArray, num,count))

      }).map(x => (x._1, x._2.mkString(",")))

    keyWords
  }

  /**
    * 社区分类
    * @param community 词社区
    * @param jsonConfig 配置文件
    * @return
    */
  def communityToClassify(community: Array[(String, Array[String])],
                          jsonConfig: JsonConfig): Array[(String, String, String, String)] = {

    val dicts = Bayes.initGrepDicts(jsonConfig.getValue("tdt", "dictsPath"))

    val classify = community.map(c => {

      val words = c._2

      val (stocks, industries, sections) = Regular.predict(words,
        dicts("stockDict"), dicts("indusDict"), dicts("sectDict"))

        (c._1, stocks, industries, sections)
    })

    classify
  }

  /**
    * 全部新闻提取 Hbase
    *
    * @param sc SparkContext
    * @param jsonPath  Hbase配置文件
    * @return 新闻集合
    */
  def getAllNews(sc: SparkContext, jsonPath:String): Array[String] = {

    val config = new JsonConfig
    config.initConfig(jsonPath)

    val hbaseConfig = HBaseUtil.getHbaseConf(config.getValue("hbase","rootDir"), config.getValue("hbase","ip"))
    val newsData = HBaseUtil.getRDD(sc, hbaseConfig).collect()

    newsData

  }

  /**
    * 主运行函数：基于TextRan和新闻源（Hbase）
    */
  def runHbase(sc: SparkContext): Unit = {

    //获取新闻并预处理
    val allNews = getAllNews(sc, "D:\\333_WordExtraction\\TextRank\\config.json")
    val stopWords = sc.textFile("D:\\333_WordExtraction\\TextRank\\stop_words_CN").collect()

    val kunyanConf = new KunyanConf()
    kunyanConf.set("222.73.57.17", 16003)

    val newsSeg = allNews.map(news=>{

      val content = news.substring(news.indexOf("&*@") + 1, news.length)
      val contentSeg = TextPreprocessing.process(content, stopWords)

      contentSeg
    })

    val newsRdd = sc.parallelize(newsSeg)

    //事件群
    val commulities = sc.textFile("D:\\333_WordExtraction\\TextRank\\part-00000_10-Community.txt").map(line => {

        val line2 = line.replace("(", "").replace(")", "")
        val commulity = line2.split(",")
        val top = commulity(0)
        commulity.toBuffer.trimStart(0)

        (top, commulity)
      }).collect()

    val commulitiesRdd = sc.parallelize(commulities)

    //基于TextRank社区关键词
    val keyWords = getKeyWordsForTotalCommunity(sc, newsRdd, commulitiesRdd,5)
    keyWords.foreach(line => println(line))

  }

  /**
    * 主运行函数  基于事件词抽取主题词
    */
  def run(sc:SparkContext): Unit = {

    //事件群
    val communitiesArr = sc.textFile("D:\\333_WordExtraction\\TextRank\\community\\community_liu_3.txt")
      .map(community => {

        val temp = community.split("\\|")
        val words = temp(2).split("\t")

        words
      }).collect()

    //事件主题词抽取
    val candiWords = SubjectWord.getCandiWordFromSpareWords(communitiesArr(1))
    LoggerUtil.warn("完成候选词提取！ 》》》》》》》》》》》》》")
    candiWords.foreach(line => println(line.length+": " + line))

    val words = SubjectWord.getWordsFromCandiWords(candiWords)
    LoggerUtil.warn("完成主题词提取！ 》》》》》》》》》》》》》")

    //主题词写出
    val resultPath = "D:\\333_WordExtraction\\TextRank\\community\\result_Subject_comWordAll.md"
    val writer=new PrintWriter(resultPath, "utf-8")
    writer.append("|\n")

    words.foreach(line => println("主题词： " + line))
  }

  def run2(sc:SparkContext): Unit = {

    //事件群
    val communitiesArr = sc.textFile("D:\\333_WordExtraction\\TextRank\\community\\community_liu_3.txt")
      .map(community => {

        val temp = community.split("\\|")
        val words = temp(2).split("\t")

        words
      }).collect()

    //事件主题词抽取
    val candiWords = SubjectWord2.getCandiWordFromSpareWords(communitiesArr)
    LoggerUtil.warn("完成候选词提取！ 》》》》》》》》》》》》》")
    candiWords.foreach(line => println(line.length+": " + line.toList))

    val words = SubjectWord2.getWordsFromCandiWords(candiWords)
    LoggerUtil.warn("完成主题词提取！ 》》》》》》》》》》》》》")

    //主题词写出
    val resultPath = "D:\\333_WordExtraction\\TextRank\\community\\result_Subject_comWordAll.md"
    val writer=new PrintWriter(resultPath, "utf-8")
    writer.append("|\n")

    words.foreach(line => println("主题词： " + line.mkString(",")))
  }
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("wordExtraction").setMaster("local")
    val sc = new SparkContext(conf)
    run(sc)

  }

}
