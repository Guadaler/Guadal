package com.kunyan.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyan.nlpsuit.sentiment.PredictWithNb
import com.kunyan.util._
import com.kunyan.util.LoggerUtil
import org.apache.hadoop.conf.Configuration
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
      .setMaster("local")                        //  ------------------------  打jar包不能指定maaster  ------------------------------
    val sc = new SparkContext(conf)

    try {
      // 连接redis
      val info = sc.textFile(args(0)).collect()
      val redis = RedisUtil.getRedis(info)

      // 连接Hbase
      val hbaseConf = HbaseUtil.getHbaseConf()

      // 读取停用词典
      val stopWords = sc.textFile(args(1)).collect()
      val stopWordsBr = sc.broadcast(stopWords)

      // 初始化词典，存入dicBuffer
      val dictUser = sc.textFile(args(2)).collect()
      val dictP = sc.textFile(args(3)).collect()
      val dictN = sc.textFile(args(4)).collect()
      val dictF = sc.textFile(args(5)).collect()

      val dicBuffer = new ArrayBuffer[Array[String]]()
      dicBuffer.append(dictUser)
      dicBuffer.append(dictP)
      dicBuffer.append(dictN)
      dicBuffer.append(dictF)

      // 创建表名，根据表名读redis
      val now = new Date()
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val time = dateFormat.format(now)

      val industryTime = "Industry_" + time                       // ------------------------------- industry -----------------------------
      val stockTime = "Stock_" + time                             // ------------------------------- stock --------------------------------
      val sectionTime = "Section_" + time                         // ------------------------------- section -------------------------------
      val newsTime = "News_" + time                               // -------------------------------- news ---------------------------------

      // 计算新闻的倾向比例，写入redis
      val list0 = countPercents(redis, industryTime, newsTime, hbaseConf, stopWordsBr, dicBuffer, args(6))
      RedisUtil.writeToRedis(redis, "industry_sentiment", list0)
      val list1 = countPercents(redis, stockTime, newsTime, hbaseConf, stopWordsBr, dicBuffer, args(6))
      RedisUtil.writeToRedis(redis, "stock_sentiment", list1)
      val list2 = countPercents(redis, sectionTime, newsTime, hbaseConf, stopWordsBr, dicBuffer, args(6))
      RedisUtil.writeToRedis(redis, "section_sentiment", list2)

//      val list = countPercentsRDD(sc, redis, industryTime, newsTime, hbaseConf, stopWords, dicBuffer, args(6))
//      RedisUtil.writeToRedis(redis, "industry_sentiment_RDD", list)

      redis.close()
//      LoggerUtil.info("close redis connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }catch {
      case e:Exception =>
        LoggerUtil.error(e.getMessage)
    } finally {
      sc.stop()
//      LoggerUtil.info("sc stop >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }

  }


  /**
    * 根据分类信息计算情感倾向的比例
    *
    * @param redis Jedis对象
    * @param classifyTable 数据表名称
    * @param newsTable 新闻数据表名称
    * @param hbaseconf hbase
    * @return 返回（存有类别-比值信息的Map）
    */
  def countPercents(redis:Jedis, classifyTable:String, newsTable:String, hbaseconf:Configuration, stopWordsBr:Broadcast[Array[String]],
                    dicBuffer:ArrayBuffer[Array[String]], modelPath:String):mutable.Map[String, String] = {

    // 获得所有类别名称
    val s = redis.hkeys(classifyTable)
    val classify = new  Array[String](s.size())
    s.toArray(classify)

    //初始化分类模型
    val model = PredictWithNb.init(modelPath)

    // 计算结果存储变量
    val result = mutable.Map[String, String]()

    // 针对每个分类，分别计算新闻的倾向比例
    for (i <- classify.indices) {
      // 计算过程中间变量
      var negaCount = 0
      var posiCount = 0
      var sum = 0.0f
      var ratio = ""

      // 获得当前分类下的所有新闻的key值
      val keys = redis.hget(classifyTable, classify(i))
      val newsKeys = keys.split(",")

      // 利用新闻的key值，从redis中依次取出新闻信息
      for (j <- Range(0, newsKeys.length)) {
        val newsInfo = redis.hget(newsTable, newsKeys(j))

        // 获得新闻的 title 和 Url
        val newsChange = new JSONObject(newsInfo)
        val newsTitle = newsChange.getString("title")
        val newsUrl = newsChange.getString("url")

        //用“url”从hbase中读取新闻正文
        val content = HbaseUtil.getValue(hbaseconf, "wk_detail", newsUrl, "basic", "content")

        // 如果匹配到正文，利用模型预测新闻的情感倾向
        if(content != "Null"){
//          content = newsTitle + content
          val value = PredictWithNb.predictWithSigle(content, model, stopWordsBr)
          if (value == "neg"){
            negaCount = negaCount + 1
          }
          else{
            posiCount = posiCount + 1
          }
        }
        // 如果匹配不到正文，利用词典预测标题的情感倾向
        else{
          val titleCut = SentiRelyDic.cut(newsTitle, dicBuffer(0))
          val value = SentiRelyDic.searchSenti(titleCut, dicBuffer(1), dicBuffer(2), dicBuffer(3))
          if (value == "neg") {
            negaCount = negaCount + 1
          }
          else {
            posiCount = posiCount + 1
          }
        }
      }
      sum = negaCount + posiCount
      println(classify(i) + " " + negaCount + " " + posiCount)
      ratio = (negaCount/sum).toString + "," + (posiCount/sum).toString
      result += (classify(i) -> ratio)
    }
    result
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


//  /**
//    * 根据分类信息计算情感倾向的比例
//    *
//    * @param redis Jedis对象
//    * @param categoryTable 数据表名称
//    * @param newsTable 新闻数据表名称
//    * @param hbaseconf hbase
//    * @param stopWords 停用词典
//    * @param dicBuffer 词典
//    * @param modelPath 模型地址
//    * @return 返回（存有类别-比值信息的Map）
//    * @author liumiao
//    */
//  def countPercentsRDD(sc:SparkContext, redis:Jedis, categoryTable:String, newsTable:String, hbaseconf:Configuration, stopWords:Array[String],
//                    dicBuffer:ArrayBuffer[Array[String]], modelPath:String):mutable.Map[String, String] = {
//
//    // 获得所有类别的新闻，Map[类别名称，Array[（url, title),(url, title),…]}
//    val allCateNewsMap = getAllCateNews(redis, categoryTable, newsTable)
//
//    //读取新闻RDD
//    val hbaseAllNews = HbaseUtil.getRDD(sc, hbaseconf)//.cache()
////    hbaseAllNews.take(5).foreach(println)
//
//    //初始化分类模型
//    val model = PredictWithNb.init(modelPath)
//    val modedBr = sc.broadcast(model)
//
//    val stopWordsBr = sc.broadcast(stopWords)
//
//    // 计算结果存储变量
//    val resultRadio = mutable.Map[String, String]()
//
//    // 对于url能够在Hbase中匹配到正文的新闻，利用分类模型预测其正文的情感倾向
//    val hbaseRedisSentiment = hbaseAllNews.map(everyNews => {
//      if (everyNews.split("\n\t").length == 3){
//        val Array(url, title, content) = everyNews.split("\n\t")
//        val categories = hasUrl(url, allCateNewsMap)
//        if (categories != "there is no cate") {
//          // 预测正文的情感倾向
//          val result = PredictWithNb.predictWithSigle(content, modedBr.value, stopWordsBr)    // 之后改成stopWords类型Array[String]  ------------------------------
//          //caregories是一个Tuple（），即一条新闻可能属于多个分类
//          (url, categories, result)
//        }
//      }
//    }).filter(_ !=()).map(_.asInstanceOf[(String, String, String)]).cache()
////    println(hbaseRedisSentiment.count())
//
//    // 抽取类别和情感倾向分析结果，返回Array[String, String]
//    val hbaseRedisResult = hbaseRedisSentiment.map(line => {
//      (line._2, line._3)
//    }).collect()
////    println(hbaseRedisResult.length)
//
//    //抽取所有交叉的url,返回Array[String]
//    val intersetUrl = hbaseRedisSentiment.map(_._1).collect()
////    println(intersetUrl.length)
//
//    // 对于redis中与hbase不交叉的新闻，利用标题计算其情感倾向
//    val result = sc.parallelize(allCateNewsMap.toSeq).map(redisNew => {
//      // redisNew (类别, Array[(url, title),(url, title),……])
//      val resultCate = redisNew._2.map(tuple => {
//        // tuple  (url, title)
//        if (!intersetUrl.contains(tuple._1)) {
//          // 预测标题的情感倾向
//          val titleCut = SentiRelyDic.cut(tuple._2, dicBuffer(0))
//          val resultTitle = SentiRelyDic.searchSenti(titleCut, dicBuffer(1), dicBuffer(2), dicBuffer(3))
//          // 返回标题的情感倾向分析结果
//          println(resultTitle)
//          resultTitle
//        }
//      }).filter(_ !=()).map(_.asInstanceOf[String]).toBuffer
//
//      println(resultCate.length)
//
//      // 将正文预测结果和标题预测结果 根据类别合并
//      hbaseRedisResult.foreach(item => {
//        if (item._1.split(",").contains(redisNew._1)) {
//          resultCate.append(item._2)
//        }
//      })
//
//      println(resultCate.length)
//
//      val pNeg = 1.0 * resultCate.count(_ == "neg") / resultCate.length
//
//      resultRadio += (redisNew._1 -> Array(pNeg, 1 - pNeg).mkString(","))
//
//      println(redisNew._1 + " " + pNeg.toString + " " + (1-pNeg).toString)
//
//      (redisNew._1, Array(pNeg, 1 - pNeg).mkString(","))
//
//    }).collect()
//
//    println(result.length)
//
//    resultRadio
//
//  }
//
//  private def RDDToMap(result:RDD[(String, String)]): Unit ={
//
//
//  }


}
