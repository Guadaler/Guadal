package com.kunyan.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyan.nlpsuit.sentiment.PredictWithNb
import com.kunyan.util._
//import com.kunyan.util.LoggerUtil
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.{Tuple, Jedis}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Liu on 2016/4/13.
  */

object NewsTrendPre {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("NewsTrendPre")
      .setMaster("local")                        //  ------------------------  打jar包不能指定Maaster  ------------------------------
    val sc = new SparkContext(conf)
//    LoggerUtil.warn("sc init success")

    // 连接redis
    val redis = RedisUtil.getRedis
//    LoggerUtil.warn("redis connet successfully")

    // 连接Hbase
    val hbaseConf = HbaseUtil.getHbaseConf
    val hConnection = ConnectionFactory.createConnection(hbaseConf)
//    LoggerUtil.warn("hbase connet successfully")

    // 读取停用词典
    val stopWords = sc.textFile(args(0)).collect()

    // 初始化词典，存入dicBuffer
    val dictUser = sc.textFile(args(1)).collect()
    val dictP = sc.textFile(args(2)).collect()
    val dictN = sc.textFile(args(3)).collect()
    val dictF = sc.textFile(args(4)).collect()

    val dicBuffer = new ArrayBuffer[Array[String]]()
    dicBuffer.append(dictUser)
    dicBuffer.append(dictP)
    dicBuffer.append(dictN)
    dicBuffer.append(dictF)

    dicBuffer.append(stopWords)
//    LoggerUtil.warn("dicts read successfully")

    //初始化分类模型
    val model = PredictWithNb.init(args(5))

    // 创建表名，根据表名读redis
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val time = dateFormat.format(now)

    val industryTime = "Industry_" + time                       // ------------------------------- industry -----------------------------
    val stockTime = "Stock_" + time                             // ------------------------------- stock --------------------------------
    val sectionTime = "Section_" + time                         // ------------------------------- section -------------------------------
    val newsTime = "News_" + time                               // -------------------------------- news ---------------------------------
//    LoggerUtil.warn("create redis table name successfully")

    // 获得redis中所有类别的新闻，存储为Map[类别名称，Array[（url, title),(url, title),…]}
    val redisAllNewsMapIndustry = getAllCateNews(redis, industryTime, newsTime)
    val redisAllNewsMapStock = getAllCateNews(redis, stockTime, newsTime)
    val redisAllNewsMapSection = getAllCateNews(redis, sectionTime, newsTime)
    redis.close()
//    LoggerUtil.info("close redis connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


    // 获得hbase中所有的新闻，存储为RDD[String]
    val hbaseAllNews = HbaseUtil.getRDD(sc, hbaseConf).cache()


    // 计算新闻的倾向比例，写入redis
    val list1 = countPercentsRDD(sc, redisAllNewsMapIndustry, hbaseAllNews, dicBuffer, model)
    val list2 = countPercentsRDD(sc, redisAllNewsMapStock, hbaseAllNews, dicBuffer, model)
    val list3 = countPercentsRDD(sc, redisAllNewsMapSection, hbaseAllNews, dicBuffer, model)
//    LoggerUtil.info("predict industry trend successfully")

    //存储到redis
    if (!redis.isConnected){
      val redis = RedisUtil.getRedis
      RedisUtil.writeToRedis(redis, "industry_sentiment", list1)
      RedisUtil.writeToRedis(redis, "stock_sentiment", list2)
      RedisUtil.writeToRedis(redis, "section_sentiment", list3)
      redis.close()
    }

    hConnection.close()
//      LoggerUtil.info("close hbase connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    sc.stop()
//    LoggerUtil.info("sc stop>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

  }


  /**
    * 根据分类信息计算情感倾向的比例
    *
    * @param dicBuffer 词典
    * @param model 分类模型
    * @return 返回（存有类别-比值信息的Map）
    * @author liumiao
    */
  def countPercentsRDD(sc:SparkContext, redisAllNewsMap:Map[String, Array[(String, String)]], hbaseAllNewsRDD:RDD[String],
                       dicBuffer:ArrayBuffer[Array[String]], model:Map[String, Any]):mutable.Map[String, String] = {

    //广播分类模型和停用词表
    val modelBr = sc.broadcast(model)
    val stopWordsBr = sc.broadcast(dicBuffer(4))

    // 对于url能够在Hbase中匹配到正文的新闻，利用分类模型预测其正文的情感倾向
    val hbaseRedisSentiment = hbaseAllNewsRDD.map(everyNews => {
      if (everyNews.split("\n\t").length == 3){
        val Array(url, title, content) = everyNews.split("\n\t")
        val categories = hasUrl(url, redisAllNewsMap)
        if (categories != "there is no cate") {
          // 预测正文的情感倾向
          val result = PredictWithNb.predictWithSigle(content, modelBr.value, stopWordsBr)    // 之后改成stopWords类型Array[String]  ------------------------------
          //caregories是一个Tuple（），即一条新闻可能属于多个分类
          (url, categories, result)
        }
      }
    }).filter(_ !=()).map(_.asInstanceOf[(String, String, String)]).cache()

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
          val resultTitle = SentiRelyDic.searchSenti(tuple._2, dicBuffer)
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
    val resultMap = mutable.Map[String, String]()
    result.map(tuple => {
      resultMap += (tuple._1 -> tuple._2)
    })

    resultMap
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
      "there is no cate"
    } else result.mkString(",")
  }


  /**
    * 读取redis的新闻信息
    *
    * @param redis
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
