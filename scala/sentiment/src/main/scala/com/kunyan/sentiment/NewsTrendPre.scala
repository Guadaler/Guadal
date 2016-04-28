package com.kunyan.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlpsuit.sentiment.{PredictWithNb, SentiRelyDic}
import com.kunyan.util._
import com.kunyan.util.LoggerUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.Jedis
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * Created by Liu on 2016/4/13.
  */

object NewsTrendPre {

  def getDicts(sentimentConf: SentimentConf): Map[String, Array[String]] = {
    // 读取停用词典
    val stopWords = Source.fromFile(sentimentConf.getValue("dicts", "stopWordsPath")).getLines().toArray

    // 初始化词典，存入dicBuffer
    val userDict = Source.fromFile(sentimentConf.getValue("dicts", "userDictPath")).getLines().toArray
    val dictP = Source.fromFile(sentimentConf.getValue("dicts", "posDictPath")).getLines().toArray
    val dictN = Source.fromFile(sentimentConf.getValue("dicts", "pasDictPath")).getLines().toArray
    val dictF = Source.fromFile(sentimentConf.getValue("dicts", "negDictPath")).getLines().toArray
    Map("stopWordsCN" -> stopWords, "userDict" -> userDict,
      "dictP" -> dictP, "dictN" -> dictN, "dictF" -> dictF)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("NewsTrendPreTest")
//      .setMaster("local")
//      .set("spark.local.ip", "192.168.2.65")
//      .set("spark.driver.host", "192.168.2.65")//  ------------------------  打jar包不能指定Maaster  ------------------------------
    val sc = new SparkContext(conf)
    LoggerUtil.warn("sc init successfully")

    // 获取配置信息
    val configInfo = new SentimentConf()
    configInfo.initConfig(args(0))

    // 连接redis
    val redisInput = RedisUtil.getRedis(configInfo)
    // 连接Hbase
    val hbaseConf = HbaseUtil.getHbaseConf(configInfo)

    // 获取词典
    val cosDicts = getDicts(configInfo)
    val stopWordsBr = sc.broadcast(cosDicts)

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

    // 获得redis中所有类别的新闻，存储为Map[类别名称，Array[（url, title),(url, title),…]}
    val redisAllNewsMapIndustry = getAllCateNews(redisInput, industryTime, newsTime)
    val redisAllNewsMapStock = getAllCateNews(redisInput, stockTime, newsTime)
    val redisAllNewsMapSection = getAllCateNews(redisInput, sectionTime, newsTime)
    redisInput.close()
    LoggerUtil.warn("get redis news successfully")

    // 获得hbase中所有的新闻，存储为RDD[String]
    val hbaseAllNews = HbaseUtil.getRDD(sc, hbaseConf).cache()
    LoggerUtil.warn("get hbase news successfully")

    // 计算新闻的倾向比例，写入redis
    val list1 = countPercentsRDD(sc, redisAllNewsMapIndustry, hbaseAllNews, stopWordsBr, modelsBr)
    val list2 = countPercentsRDD(sc, redisAllNewsMapStock, hbaseAllNews, stopWordsBr, modelsBr)
    val list3 = countPercentsRDD(sc, redisAllNewsMapSection, hbaseAllNews, stopWordsBr, modelsBr)
//    LoggerUtil.info("predict industry trend successfully")

    //存储到redis
    val redisOutput = RedisUtil.getRedis(configInfo)
    RedisUtil.writeToRedis(redisOutput, "industry_sentiment", list1)
    RedisUtil.writeToRedis(redisOutput, "stock_sentiment", list2)
    RedisUtil.writeToRedis(redisOutput, "section_sentiment", list3)
    redisInput.close()
    LoggerUtil.warn("close redis connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    sc.stop()
    LoggerUtil.warn("sc stop>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

  }

  /**
    * 根据分类信息计算情感倾向的比例
    *
    * @param dicMapBr 词典
    * @param modelsbr 分类模型
    * @return 返回（存有类别-比值信息的Map）
    * @author liumiao
    */
  def countPercentsRDD(sc:SparkContext, redisAllNewsMap:Map[String, Array[(String, String)]], hbaseAllNewsRDD:RDD[String],
                       dicMapBr:Broadcast[Map[String, Array[String]]], modelsbr:Broadcast[Map[String, Any]]): Map[String, String] = {

    // 对于url能够在Hbase中匹配到正文的新闻，利用分类模型预测其正文的情感倾向
    val hbaseRedisSentiment = hbaseAllNewsRDD.map(everyNews => {
      if (everyNews.split("\n\t").length == 3){
        val Array(url, title, content) = everyNews.split("\n\t")
        val categories = hasUrl(url, redisAllNewsMap)
        if (categories != "there is no this url") {
          // 预测正文的情感倾向
          val result = PredictWithNb.predictWithSigle(content, modelsbr.value, dicMapBr.value("stopWordsCN"))    // 之后改成stopWords类型Array[String]  ------------------------------
          //caregories是一个Tuple（），即一条新闻可能属于多个分类
          (url, categories, result)
        }
      }
    }).filter(_ !=()).map(_.asInstanceOf[(String, String, String)]).cache()
    LoggerUtil.warn("hbaseRedisSentiment process success")


    // 抽取类别和情感倾向分析结果，返回Array[String, String]
    val hbaseRedisResult = hbaseRedisSentiment.map(line => {
      (line._2, line._3)
    }).collect()

    //抽取所有交叉的url,返回Array[String]
    val intersetUrl = hbaseRedisSentiment.map(_._1).collect()

    // 对于redis中与hbase不交叉的新闻，利用标题计算其情感倾向
    val result = sc.parallelize(redisAllNewsMap.toSeq).map(redisNew => {
      // redisNew (类别, Array[(url, title),(url, title),……])
      val resultCate = redisNew._2.map(tuple => {
        // tuple  (url, title)
        if (!intersetUrl.contains(tuple._1)) {
          // 预测标题的情感倾向
          val resultTitle = SentiRelyDic.searchSenti(tuple._2, dicMapBr.value)
          // 返回标题的情感倾向分析结果
          resultTitle
        }
      }).filter(_ !=()).map(_.asInstanceOf[String]).toBuffer

      // 将正文预测结果和标题预测结果 根据类别合并
      hbaseRedisResult.foreach(item => {
        if (item._1.split(",").contains(redisNew._1)) {
          resultCate.append(item._2)
        }
      })

      // 根据分类计算负面和非负面的新闻的比例
      val pNeg = 1.0 * resultCate.count(_ == "neg") / resultCate.length
      (redisNew._1, Array(pNeg, 1 - pNeg).mkString(","))
    }).collect()

//    result.foreach(println)

    // 将Array转换为Map
    result.toMap
  }


  /**
    * 匹配redis中是否包含此条url的信息
    *
    * @param url 待匹配的url
    * @param cateMap  Map[类别名称，Array[（url, title）]}，注意一个类别中问问包含多条新闻
    * @return url的行业信息，注意一条新闻有时分布于多个不同的类别中
    * @author liumiao
    */
  private def hasUrl(url: String, cateMap: Map[String, Array[(String, String)]]): String = {
    val result = ArrayBuffer[String]()
    cateMap.foreach(line => {
      // 判断当前类别中的所有新闻是否包含指定的url
      val temp = line._2.map(_._1).contains(url)
      if (temp){
        result.append(line._1)
      }
    })
    // 返回String
    if (result.isEmpty){
      "there is no this url"
    } else result.mkString(",")
  }


  /**
    * 读取redis的新闻信息
    *
    * @param redis redis 链接信息
    * @param categoryTable 类别表名
    * @param newsTable  新闻表名
    * @return 新闻信息，返回Map[类别名称，Array[（url, title),(url, title),…]}
    */
  private  def getAllCateNews(redis:Jedis, categoryTable:String, newsTable:String): Map[String, Array[(String, String)]] ={
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
