package com.kunyan.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyan.nlpsuit.sentiment.PredictWithNb
import com.kunyan.util._
import com.kunyan.util.LoggerUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.Jedis

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
    LoggerUtil.warn("sc init success")

    // 连接 redis 和 Hbase
    val redis = RedisUtil.getRedis
    val hbaseConf = HBaseUtil.getHbaseConf

    // 读取用户自定义词典，添加到分词系统
    val dictUser = sc.textFile(args(0)).collect()
    SentiRelyDic.addUserDic(dictUser)

    // 初始化词典，存入dicBuffer
    val dictP = sc.textFile(args(1)).collect()
    val dictN = sc.textFile(args(2)).collect()
    val dictF = sc.textFile(args(3)).collect()
    val stopWords = sc.textFile(args(4)).collect()

    val dicMap = mutable.Map[String, Array[String]]()
    dicMap += ("dicPosi" -> dictP)
    dicMap += ("dicNega" -> dictN)
    dicMap += ("dicF" -> dictF)
    dicMap += ("dicStop" -> stopWords)
    LoggerUtil.warn("dicts read successfully")

    //初始化分类模型
    val model = PredictWithNb.init(args(5))
    LoggerUtil.warn("model init successfully")

    // 创建表名，根据表名读redis
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val time = dateFormat.format(now)

    val industryTime = "Industry_" + time                       // ------------------------------- industry -----------------------------
    val stockTime = "Stock_" + time                             // ------------------------------- stock --------------------------------
    val sectionTime = "Section_" + time                         // ------------------------------- section -------------------------------
    val newsTime = "News_" + time                               // -------------------------------- news ---------------------------------

    // 获得redis中所有类别的新闻，存储为Map[类别名称，Array[（url, title),(url, title),…]}
    val redisAllNewsMapIndustry = getAllCateNews(redis, industryTime, newsTime)
    val redisAllNewsMapStock = getAllCateNews(redis, stockTime, newsTime)
    val redisAllNewsMapSection = getAllCateNews(redis, sectionTime, newsTime)
    redis.close()
    LoggerUtil.warn("get redis news successfully")

    // 获得hbase中所有的新闻，存储为RDD[String]
    val hbaseAllNews = HBaseUtil.getRDD(sc, hbaseConf).cache()
    LoggerUtil.warn("read hbase news RDD successfully")

//    val modelBr = sc.broadcast(model)
//    val stopWordsBr = sc.broadcast(dicMap("dicStop"))
//    val hbaseRedisSentiment = getIntersectResult(hbaseAllNews, redisAllNewsMapIndustry, modelBr.value, stopWordsBr)
//    LoggerUtil.warn("get intersect news result successfully")
//
//    println(hbaseRedisSentiment.count())
//
//    //抽取所有交叉的url,返回Array[String]
//    val intersectUrl = hbaseRedisSentiment.map(_._1).collect()
//    println(intersectUrl.length)
//
//    // 抽取类别和情感倾向分析结果，返回Array[String, String]
//    val hbaseRedisResult = hbaseRedisSentiment.map(line => {
//      (line._2, line._3)
//    }).collect()
//    println(hbaseRedisResult.length)
//
//    val redisNewsMap = sc.parallelize(redisAllNewsMapIndustry.toSeq)
//    val result = getUnintersectResult(redisNewsMap, hbaseRedisResult, intersectUrl, dicMap)
//    LoggerUtil.warn("get unintersect news result successfully")


    // 计算新闻的倾向比例，写入redis
    val list1 = countPercentsRDD(sc, redisAllNewsMapIndustry, hbaseAllNews, dicMap, model)
    val list2 = countPercentsRDD(sc, redisAllNewsMapStock, hbaseAllNews, dicMap, model)
    val list3 = countPercentsRDD(sc, redisAllNewsMapSection, hbaseAllNews, dicMap, model)
    LoggerUtil.warn("predict trend successfully")

    //存储到redis
    val redis2 = RedisUtil.getRedis
    RedisUtil.writeToRedis(redis2, "industry_sentiment", list1)
    RedisUtil.writeToRedis(redis2, "stock_sentiment", list2)
    RedisUtil.writeToRedis(redis2, "section_sentiment", list3)
    LoggerUtil.warn("write to redis successfully")

    redis2.close()
    LoggerUtil.warn("close redis connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    sc.stop()
    LoggerUtil.warn("sc stop>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

  }

  /**
    *
    */
  private def getIntersectResult(hbaseAllNewsRDD:RDD[String], redisAllNewsMap:Map[String, Array[(String, String)]], modelBr:Map[String, Any],
                                 stopWordsBr:Broadcast[Array[String]]): RDD[(String, String, String)] = {

    // 对于url能够在Hbase中匹配到正文的新闻，利用分类模型预测其正文的情感倾向
    val hbaseRedisSentiment = hbaseAllNewsRDD.map(everyNews => {
      if (everyNews.split("\n\t").length == 3){
        val Array(url, title, content) = everyNews.split("\n\t")
        val categories = hasUrl(url, redisAllNewsMap)
        if (categories != "there is no this url") {
          // 预测正文的情感倾向
          val result = PredictWithNb.predictWithSigle(content, modelBr, stopWordsBr)    // 之后改成stopWords类型Array[String]  ------------------------------
          //categories是一个Tuple（），即一条新闻可能属于多个分类
          (url, categories, result)
        }
      }
    }).filter(_ !=()).map(_.asInstanceOf[(String, String, String)]).cache()

    hbaseRedisSentiment
  }


  /**
    *
    */
  private def getUnintersectResult(redisAllNewsMap:RDD[(String, Array[(String, String)])], hbaseRedisResult:Array[(String,String)],
                                   intersectUrl:Array[String], dicMap:mutable.Map[String, Array[String]]): Array[(String, Array[String])] ={
    // 对于redis中与hbase不交叉的新闻，利用标题计算其情感倾向
    val result = redisAllNewsMap.map(redisNew => {
      // redisNew (类别, Array[(url, title),(url, title),……])
      val resultCate = redisNew._2.map(tuple => {
        // tuple  (url, title)
        if (!intersectUrl.contains(tuple._1)) {
          // 预测标题的情感倾向
          val resultTitle = SentiRelyDic.searchSenti(tuple._2, dicMap)
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

      (redisNew._1, resultCate.toArray)

    }).collect()

    result
  }





  /**
    * 根据分类信息计算情感倾向的比例
    *
    * @param dicMap 词典
    * @param model 分类模型
    * @return 返回（存有类别-比值信息的Map）
    * @author liumiao
    */
  def countPercentsRDD(sc:SparkContext, redisAllNewsMap:Map[String, Array[(String, String)]], hbaseAllNewsRDD:RDD[String],
                       dicMap:mutable.Map[String, Array[String]], model:Map[String, Any]):mutable.Map[String, String] = {

    //广播分类模型和停用词表
    val modelBr = sc.broadcast(model)
    val stopWordsBr = sc.broadcast(dicMap("dicStop"))

    // 对于url能够在Hbase中匹配到正文的新闻，利用分类模型预测其正文的情感倾向
    val hbaseRedisSentiment = hbaseAllNewsRDD.map(everyNews => {
      if (everyNews.split("\n\t").length == 3){
        val Array(url, title, content) = everyNews.split("\n\t")
        val categories = hasUrl(url, redisAllNewsMap)
        if (categories != "there is no this url") {
          // 预测正文的情感倾向
          val result = PredictWithNb.predictWithSigle(content, modelBr.value, stopWordsBr)    // 之后改成stopWords类型Array[String]  ------------------------------
          //categories是一个Tuple（），即一条新闻可能属于多个分类
          (url, categories, result)
        }
      }
    }).filter(_ !=()).map(_.asInstanceOf[(String, String, String)]).cache()

    // 抽取类别和情感倾向分析结果，返回Array[String, String]
    val hbaseRedisResult = hbaseRedisSentiment.map(line => {
      (line._2, line._3)
    }).collect()

    //抽取所有交叉的url,返回Array[String]
    val intersectUrl = hbaseRedisSentiment.map(_._1).collect()

    // 对于redis中与hbase不交叉的新闻，利用标题计算其情感倾向
    val result = sc.parallelize(redisAllNewsMap.toSeq).map(redisNew => {
      // redisNew (类别, Array[(url, title),(url, title),……])
      val resultCate = redisNew._2.map(tuple => {
        // tuple  (url, title)
        if (!intersectUrl.contains(tuple._1)) {
          // 预测标题的情感倾向
          val resultTitle = SentiRelyDic.searchSenti(tuple._2, dicMap)
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

    result.foreach(println)

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
      "there is no this url"
    } else result.mkString(",")
  }


  /**
    * 读取redis的新闻信息
    *
    * @param redis redis资源
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
