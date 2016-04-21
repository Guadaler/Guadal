package com.kunyan.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyan.nlpsuit.sentiment.PredictWithNb
import com.kunyan.util._
import com.kunyan.util.LoggerUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by Liu on 2016/4/13.
  */

object NewsTrendPre {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("News_trend_pre")
//      .setMaster("local")
    val sc = new SparkContext(conf)

    try {

      // 连接redis
      val redis = RedisUtil.getRedis(sc, args(0))
      val hbaseConf = HbaseUtil.getHbaseConf()

      // 读取停用词典
      val stopWords = sc.textFile(args(1)).collect()
      val stopWordsBr = sc.broadcast(stopWords)

      // 创建表名，根据表名读redis
      val now = new Date()
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val time = dateFormat.format(now)

      val industryTime = "Industry_" + time                        // ------------------------------- industry -----------------------------
      val stockTime = "Stock_" + time                           // ------------------------------- stock --------------------------------
      val sectionTime = "Section_" + time                        // ------------------------------- section -------------------------------
      val newsTime = "News_" + time                           // -------------------------------- news ---------------------------------

      // 计算新闻的倾向比例
      val list0 = count_percents(sc, redis, industryTime, newsTime, hbaseConf, stopWordsBr, args(2), args(3), args(4), args(5), args(6))
      val list1 = count_percents(sc, redis, stockTime, newsTime, hbaseConf, stopWordsBr, args(2), args(3), args(4), args(5), args(6))
      val list2 = count_percents(sc, redis, sectionTime, newsTime, hbaseConf, stopWordsBr, args(2), args(3), args(4), args(5), args(6))

      // 写入redis
      RedisUtil.writeToRedis(redis, "industry_sentiment", list0)
      RedisUtil.writeToRedis(redis, "stock_sentiment", list1)
      RedisUtil.writeToRedis(redis, "section_sentiment", list2)

      redis.close()
//      println("close redis connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    }catch {
      case e:Exception =>
        LoggerUtil.error(e.getMessage)
//        println(e.getMessage)
    } finally {
      sc.stop()
//      println("sc stop >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
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
  def count_percents(sc:SparkContext, redis:Jedis, classifyTable:String, newsTable:String, hbaseconf:Configuration, stopWordsBr:Broadcast[Array[String]],
                     dicUser:String, dicPosi:String, dicNega:String, dicN:String, modelPath:String):mutable.Map[String, String] = {

    // 获得所有类别名称
    val s = redis.hkeys(classifyTable)
    val classify = new  Array[String](s.size())
    s.toArray(classify)

    //初始化分类模型
    val model = PredictWithNb.init(modelPath)

    // 读取新闻RDD
    val allNews = HbaseUtil.getRDD(sc, hbaseconf)

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
        var content = "Null"

//        content = HbaseUtil.getValue(hbaseconf, "wk_detail", newsUrl, "basic", "content")

        //RDD方式读取
        val newsFilter = allNews.filter( x => {
          val newsSplit = x.split("\n\t")
          newsSplit(0) == newsUrl
        })

        if(newsFilter.count() != 0){
          newsFilter.take(1).foreach( x => {
            val oneNews = x.split("\n\t")
            content = oneNews(2)
          })
        }
        //        println(content)

        // 如果匹配到正文，利用模型预测正文的情感倾向
        if(content != "Null"){
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
          val title_cut = SentiRelyDic.cut(sc, newsTitle, dicUser)
          val value = SentiRelyDic.searchSenti(sc, title_cut, dicPosi, dicNega, dicN)
          if (value < 0) {
            negaCount = negaCount + 1
          }
          else {
            posiCount = posiCount + 1
          }
        }
      }
      sum = negaCount + posiCount
//      println(classify(i) + " " + negaCount + " " + posiCount)

      ratio = (negaCount/sum).toString + "," + (posiCount/sum).toString
      result += (classify(i) -> ratio)
    }
    result
  }

}
