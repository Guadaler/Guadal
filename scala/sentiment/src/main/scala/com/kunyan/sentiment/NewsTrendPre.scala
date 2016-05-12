package com.kunyan.sentiment

import com.kunyan.util._
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.{TextPreprocessing, KunyanConf}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.io.Source

/**
  * Created by Administrator on 2016/5/4.
  * 情感分析项目主流程类
  */
object NewsTrendPre {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("NewsTrendPre").setMaster("local")
    val sc = new SparkContext(conf)
    LoggerUtil.warn("sc init successfully")

    // 获取配置文件信息
    val configInfo = new SentimentConf()
    configInfo.initConfig(args(0))

    // 连接redis、hBase、MySql
    val redisInput = RedisUtil.getRedis(configInfo)
    val hbaseConf = HBaseUtil.getHbaseConf(configInfo)
    val sqlContent = new SQLContext(sc)

    // 获取词典
    val dictBr = sc.broadcast(getDicts(configInfo))

    // 配置kunyan分词
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    // 添加自定义词典到ansj分词器中
    SentiRelyDic.addUserDic(dictBr.value("userDict"))

    // 初始化分类模型
    val models = PredictWithNb.init(configInfo.getValue("models", "sentModelsPath"))
    val modelsBr = sc.broadcast(models)

    // 创建表名，根据表名读redis
    val time = TimeUtil.get_date("yyyyMMdd")
    val industryTable = "Industry_" + time                      
    val stockTable = "Stock_" + time                             
    val sectionTable = "Section_" + time                       
    val newsTable = "News_" + time          

    // 读取redis中所有的新闻，存储为Array[(url, title)]
    val redisAllNews = getAllNews(redisInput, newsTable)
    LoggerUtil.warn("redisAllNews = " + redisAllNews.length.toString)

    // 获得redis中所有类别的新闻，存储为Map[类别名称，Array[（url, title),(url, title),…]}
    val allIndustryNews = getAllCateNews(redisInput, industryTable, newsTable)
    val allStockNews = getAllCateNews(redisInput, stockTable, newsTable)
    val allSectionNews = getAllCateNews(redisInput, sectionTable, newsTable)
    redisInput.close()
    LoggerUtil.warn("close redisInput successfully >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    // 获得hbase中所有的新闻，存储为RDD[String]
    val hbaseAllNews = HBaseUtil.getRDD(sc, hbaseConf).cache()
    val hNewsSenti = hBaseNewsTrend(hbaseAllNews, dictBr, modelsBr, kunyanConfig)
    LoggerUtil.warn("hbaseAllNews = " + hbaseAllNews.count().toString  + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    // 计算每篇新闻的情感倾向，并写入MySQL
    val everyNewsSentiment = predictNewsTrend(sc, redisAllNews, hNewsSenti, dictBr, kunyanConfig)
    val dateNow = TimeUtil.get_date("yyyy-MM-dd HH:mm:ss")
    val data = sc.parallelize(everyNewsSentiment.toSeq).map(x =>  Row(x._1, dateNow, x._2))
    MySQLUtil.writeToMyaql(configInfo, sqlContent, "every_news_trend", data)
    LoggerUtil.warn("predict news trend and write to mysql successfully >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    // 计算新闻的倾向比例，并写入redis
    val list1 = countCatePercents(sc, allIndustryNews, everyNewsSentiment)
    val list2 = countCatePercents(sc, allStockNews, everyNewsSentiment)
    val list3 = countCatePercents(sc, allSectionNews, everyNewsSentiment)

    val redisOutput = RedisUtil.getRedis(configInfo)
    RedisUtil.writeToRedis(redisOutput, "industry_sentiment", list1)
    RedisUtil.writeToRedis(redisOutput, "stock_sentiment", list2)
    RedisUtil.writeToRedis(redisOutput, "section_sentiment", list3)
    LoggerUtil.warn("computer category percent and write to redis successfully >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    redisOutput.close()
    LoggerUtil.warn("close redisOutput connection >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    sc.stop()
    LoggerUtil.warn("sc stop >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

  }

  /**
    * 计算hBase与redis中所有新闻的情感倾向
    *
    * @param sc spark程序入口
    * @param redisAllNews redis中所有的新闻
    * @param hNewsSentiment hBase中所有的新闻情感倾向
    * @param dictBr 词典
    * @return 每条新闻的（url，time, sentiment）
    * @author liumiao
    */
  def predictNewsTrend(sc: SparkContext, redisAllNews: Array[(String, String)], hNewsSentiment: Array[(String, String)],
                       dictBr: Broadcast[Map[String, Array[String]]], kunyanConf: KunyanConf): Map[String, String] = {

    // 对于redis中url与hBase不交叉的新闻，根据标题预测情感倾向
    val confBr = sc.broadcast(kunyanConf)

    val redisNewsSentiment = sc.parallelize(redisAllNews.toSeq).map(news => {

      if (!hNewsSentiment.map(_._1).contains(news._1)) {

        val resultTitle = SentiRelyDic.predictSenti(news._2, dictBr.value, confBr.value)

        if (resultTitle == "neg") {
          (news._1, "0")
        } else {
          (news._1, "1")
        }

      }

    }).filter(_ !=()).map(_.asInstanceOf[(String, String)]).collect()
    LoggerUtil.warn( "un intersect news = " + redisNewsSentiment.length.toString)

    // 合并hBase和redis的新闻
    val everyNewsSentiment = hNewsSentiment.toBuffer
    redisNewsSentiment.foreach( result => {
      everyNewsSentiment.append(result)
    })
    LoggerUtil.warn("All news = " + everyNewsSentiment.length.toString)

    everyNewsSentiment.toMap
  }

  /**
    * 预测hBase中所有新闻的情感倾向
    *
    * @param hBaseAllNews hBase中取出的所有新闻
    * @param dictBr 词典
    * @param modelsBr 分类器模型
    * @return 新闻情感倾向
    * @author liumiao
    */
  private def hBaseNewsTrend(hBaseAllNews: RDD[String], dictBr: Broadcast[Map[String, Array[String]]], modelsBr: Broadcast[Map[String, Any]],
                             kunyanConfig: KunyanConf): Array[(String, String)] ={

    val hBaseNewsSenti = hBaseAllNews.map(everyNews => {

      if (everyNews.split("\n\t").length == 3) {

        val arr = everyNews.split("\n\t")
        val resultContent = PredictWithNb.predictWithSigle(arr(2), modelsBr.value, dictBr.value("stopWordsCN"), kunyanConfig)

        if (resultContent == "neg") {
          (arr(0), "0")
        } else{
          (arr(0), "1")
        }

      }

    }).filter(_ !=()).map(_.asInstanceOf[(String, String)]).collect()

    hBaseNewsSenti
  }

  /**
    * 根据分类信息计算情感倾向的比例
    *
    * @param sc spark程序入口
    * @param allCateNews 一个类别的新闻
    * @param everyNewsSentiment 每条新闻的情感值
    * @return 存有类别-比值信息的Map
    * @author liumiao
    */
  def countCatePercents(sc: SparkContext, allCateNews: Map[String, Array[(String, String)]],
                        everyNewsSentiment: Map[String, String]): Map[String, String] = {

    val urls = everyNewsSentiment.keys.toSeq
    // 对于redis中与hbase不交叉的新闻，利用标题计算其情感倾向
    val result = sc.parallelize(allCateNews.toSeq).map(oneCateNews => {

      // oneCateNew (类别, Array[(url, title),(url, title),……])
      val resultCate = oneCateNews._2.map(tuple => {

        // tuple (url, title)，everyNewsSentiment Map[url, sentiment]
        if (urls.contains(tuple._1))
          everyNewsSentiment(tuple._1)

      }).filter(_ !=()).map(_.asInstanceOf[String])

      // 根据分类计算负面和非负面的新闻的比例
      val pNeg = 1.0 * resultCate.count(_ == "0") / resultCate.length
      (oneCateNews._1, Array(pNeg, 1 - pNeg).mkString(","))

    }).collect()

    result.toMap
  }

  /**
    * 读取词典
    *
    * @param sentimentConf 配置文件
    * @return 词典
    * @author QQ
    */
  private def getDicts(sentimentConf: SentimentConf): Map[String, Array[String]] = {

    // 初始化词典，存入dicBuffer
    val stopWords = Source.fromFile(sentimentConf.getValue("dicts", "stopWordsPath")).getLines().toArray
    val userDict = Source.fromFile(sentimentConf.getValue("dicts", "userDictPath")).getLines().toArray
    val dictP = Source.fromFile(sentimentConf.getValue("dicts", "posDictPath")).getLines().toArray
    val dictN = Source.fromFile(sentimentConf.getValue("dicts", "pasDictPath")).getLines().toArray
    val dictF = Source.fromFile(sentimentConf.getValue("dicts", "negDictPath")).getLines().toArray

    Map("stopWordsCN" -> stopWords, "userDict" -> userDict, "dictP" -> dictP, "dictN" -> dictN, "dictF" -> dictF)
  }

  /**
    *  读取所有的新闻 （url, title）
    *
    * @param redis redis资源
    * @param newsTable 数据表
    * @return 所有新闻的标题和url
    * @author liumiao
    */
  def getAllNews(redis:Jedis, newsTable:String): Array[(String, String)] = {

    val newsID = redis.hkeys(newsTable).toArray.map(_.asInstanceOf[String])
    val allNews = newsID.map( id => {

      val newsJson = new JSONObject(redis.hget(newsTable, id))
      val title = newsJson.getString("title")
      val url = newsJson.getString("url")
      (url, title)

    })

    allNews
  }

  /**
    * 读取redis的新闻信息
    *
    * @param redis redis 链接信息
    * @param categoryTable 类别表名
    * @param newsTable  新闻表名
    * @return 新闻信息，返回Map[类别名称，Array[（url, title),(url, title),…]}
    * @author liumiao
    */
  private  def getAllCateNews(redis: Jedis, categoryTable: String, newsTable: String): Map[String, Array[(String, String)]] = {

    // 获得所有类别的名称
    val categoryKeys = redis.hkeys(categoryTable).toArray.map(_.asInstanceOf[String])

    // 获取每个类别中的所有新闻ID，并根据ID读取每条新闻的 title 和 url
    val cateMap = categoryKeys.map(key => {

      val newsID = redis.hget(categoryTable, key).split(",")
      val newsInfo = newsID.map(id => {

        val newJson = new JSONObject(redis.hget(newsTable, id))
        val title = newJson.getString("title")
        val url = newJson.getString("url")
        (url, title)

      })
      (key, newsInfo)       // (类别， Array[(url, title),(url, title),…]

    }).toMap

    cateMap
  }

}
