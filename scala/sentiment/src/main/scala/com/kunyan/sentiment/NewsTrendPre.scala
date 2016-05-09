package com.kunyan.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyan.util._
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
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

    val conf = new SparkConf()
      .setAppName("NewsTrendPre")

    val sc = new SparkContext(conf)
    LoggerUtil.warn("sc init successfully")

    // 获取配置信息
    val configInfo = new SentimentConf()
    configInfo.initConfig(args(0))

    // 连接redis
    val redisInput = RedisUtil.getRedis(configInfo)
    // 连接Hbase
    val hbaseConf = HBaseUtil.getHbaseConf(configInfo)
    // 连接MySql
    val sqlContent = new SQLContext(sc)

    // 获取词典
    val cosDicts = getDicts(configInfo)
    val dicWordsBr = sc.broadcast(cosDicts)

    // 添加自定义词典到ansj分词器中
    SentiRelyDic.addUserDic(cosDicts("userDict"))

    //初始化分类模型
    val models = PredictWithNb.init(configInfo.getValue("models", "sentModelsPath"))
    val modelsBr = sc.broadcast(models)

    // 创建表名，根据表名读redis
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val time = dateFormat.format(now)

    val industryTime = "Industry_" + time                       // ------------------------------- industry -----------------------------
    val stockTime = "Stock_" + time                             // ------------------------------- stock --------------------------------
    val sectionTime = "Section_" + time                         // ------------------------------- section -------------------------------
    val newsTime = "News_" + time                               // -------------------------------- news ---------------------------------

    // 读取redis中所有的新闻，存储为Array[(url, title)]
    val redisAllNews = getAllNews(redisInput, newsTime)
    LoggerUtil.warn("redisAllNews = " + redisAllNews.length.toString)

    // 获得redis中所有类别的新闻，存储为Map[类别名称，Array[（url, title),(url, title),…]}
    val allIndustryNews = getAllCateNews(redisInput, industryTime, newsTime)
    val allStockNews = getAllCateNews(redisInput, stockTime, newsTime)
    val allSectionNews = getAllCateNews(redisInput, sectionTime, newsTime)
    redisInput.close()
    LoggerUtil.warn("close redisInput successfully >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    // 获得hbase中所有的新闻，存储为RDD[String]
    val hbaseAllNews = HBaseUtil.getRDD(sc, hbaseConf).cache()
    LoggerUtil.warn("hbaseAllNews = " + hbaseAllNews.count().toString)

    // 计算每篇新闻的情感倾向，写入MySQL
    val everyNewsSentiment = predictNewsTrend(sc, redisAllNews, hbaseAllNews, dicWordsBr, modelsBr)
    val dateNow = TimeUtil.get_date("yyyy-MM-dd HH:mm:ss")
    val data = sc.parallelize(everyNewsSentiment.toSeq).map(x =>  Row(x._1, dateNow, x._2))
    LoggerUtil.warn("predict news trend successfully")

    MySQLUtil.writeToMyaql(configInfo, sqlContent, "every_news_trend", data)
    LoggerUtil.warn("write to mysql successfully")

    // 计算新闻的倾向比例，写入redis
    val list1 = countCatePercents(sc, allIndustryNews, everyNewsSentiment)
    val list2 = countCatePercents(sc, allStockNews, everyNewsSentiment)
    val list3 = countCatePercents(sc, allSectionNews, everyNewsSentiment)
    LoggerUtil.warn("computer category percent successfully")

    //存储到redis
    val redisOutput = RedisUtil.getRedis(configInfo)
    RedisUtil.writeToRedis(redisOutput, "industry_sentiment", list1)
    RedisUtil.writeToRedis(redisOutput, "stock_sentiment", list2)
    RedisUtil.writeToRedis(redisOutput, "section_sentiment", list3)
    LoggerUtil.warn("write to redis successfully")

    redisOutput.close()
    LoggerUtil.warn("close redisOutput connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    sc.stop()
    LoggerUtil.warn("sc stop>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

  }

  /**
    * 计算hbase与redis中所有新闻的情感倾向
    * @param sc spark程序入口
    * @param redisAllNews redis中所有的新闻
    * @param hbaseAllNews hbase中所有的新闻
    * @param dicMapBr 词典
    * @param modelsbr 分类模型
    * @return 每条新闻的（url，time, sentiment）
    * @author liumiao
    */
  def predictNewsTrend(sc: SparkContext, redisAllNews: Array[(String, String)], hbaseAllNews: RDD[String],
                       dicMapBr: Broadcast[Map[String, Array[String]]], modelsbr: Broadcast[Map[String, Any]]): Map[String, String] = {

    // 对于Hbase中的新闻，利用分类模型预测其正文的情感倾向
    val hbaseNewsSentiment = hbaseAllNews.map(everyNews => {

      if (everyNews.split("\n\t").length == 3) {

        val Array(url, title, content) = everyNews.split("\n\t")
        // 预测正文的情感倾向
        val resultContent = PredictWithNb.predictWithSigle(content, modelsbr.value, dicMapBr.value("stopWordsCN"))

        if (resultContent == "neg") {
          (url, "0")
        } else{
          (url, "1")
        }

      }

    }).filter(_ !=()).map(_.asInstanceOf[(String, String)]).collect()

    // 对于redis中url与hbase不交叉的新闻，根据标题预测情感倾向
    val redisNewsSentiment = sc.parallelize(redisAllNews.toSeq).map(news => {

      if (! hbaseNewsSentiment.map(_._1).contains(news._1)) {

        // 预测标题的情感倾向
        val resultTitle = SentiRelyDic.searchSenti(news._2, dicMapBr.value)

        if (resultTitle == "neg") {
          (news._1, "0")
        } else {
          (news._1, "1")
        }

      }

    }).filter(_ !=()).map(_.asInstanceOf[(String, String)]).collect()
    LoggerUtil.warn( "un intersect news = " + redisNewsSentiment.length.toString)

    // 合并hbase和redis的新闻
    val everyNewsSentiment = hbaseNewsSentiment.toBuffer
    redisNewsSentiment.foreach( result => {
      everyNewsSentiment.append(result)
    })
    LoggerUtil.warn("All news = " + everyNewsSentiment.length.toString)

    everyNewsSentiment.toMap

  }

  /**
    * 根据分类信息计算情感倾向的比例
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

    // 将Array转换为Map
    result.toMap

  }

  /**
    * 读取词典
    * @param sentimentConf 配置文件
    * @return 词典
    * @author QQ
    */
  private def getDicts(sentimentConf: SentimentConf): Map[String, Array[String]] = {

    // 读取停用词典
    val stopWords = Source.fromFile(sentimentConf.getValue("dicts", "stopWordsPath")).getLines().toArray
    // 初始化词典，存入dicBuffer
    val userDict = Source.fromFile(sentimentConf.getValue("dicts", "userDictPath")).getLines().toArray
    val dictP = Source.fromFile(sentimentConf.getValue("dicts", "posDictPath")).getLines().toArray
    val dictN = Source.fromFile(sentimentConf.getValue("dicts", "pasDictPath")).getLines().toArray
    val dictF = Source.fromFile(sentimentConf.getValue("dicts", "negDictPath")).getLines().toArray

    Map("stopWordsCN" -> stopWords, "userDict" -> userDict, "dictP" -> dictP, "dictN" -> dictN, "dictF" -> dictF)

  }


  /**
    *  读取所有的新闻 （url, title）
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
    * @param redis redis 链接信息
    * @param categoryTable 类别表名
    * @param newsTable  新闻表名
    * @return 新闻信息，返回Map[类别名称，Array[（url, title),(url, title),…]}
    * @author liumiao
    */
  private  def getAllCateNews(redis: Jedis, categoryTable: String, newsTable: String): Map[String, Array[(String, String)]] = {

    // 获得所有类别的名称（即Key值）
    val categoryKeys = redis.hkeys(categoryTable).toArray.map(_.asInstanceOf[String])
    // 根据类别的名称，获得每个类别中所有的新闻ID
    val cateMap = categoryKeys.map(key => {

      val newsID = redis.hget(categoryTable, key).split(",")

      //根据新闻ID，取出每条新闻的“url”和“title”
      val newsInfo = newsID.map(id => {

        val newJson = new JSONObject(redis.hget(newsTable, id))
        val title = newJson.getString("title")
        val url = newJson.getString("url")
        (url, title)

      })
      // 返回类别和该类别下的所有新闻的title和url
      (key, newsInfo)

    }).toMap

    // Map[类别名称，Array[（url, title),(url, title),…]}
    cateMap

  }

}
