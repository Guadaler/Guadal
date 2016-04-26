package com.kunyan.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlpsuit.sentiment.{PredictWithNb, SentiRelyDic}
import com.kunyan.util._
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.{Jedis, Tuple}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by Liu on 2016/4/13.
  */

object NewsTrendPre {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("NewsTrendPre1")
      .setMaster("local")
      .set("spark.local.ip", "192.168.2.65")
      .set("spark.driver.host", "192.168.2.65")

    //  ------------------------  打jar包不能指定Maaster  ------------------------------
    val sc = new SparkContext(conf)
//    LoggerUtil.info("sc init success")

//    try {
      // 连接redis
//      val info = sc.textFile("file:///home/sentiment/conf/redis_info.txt").collect()
      val info = Source.fromFile("/home/sentiment/conf/redis_info.txt").getLines().toArray
      val redis = RedisUtil.getRedis(info)
//      LoggerUtil.info("redis connet successfully")

      // 连接Hbase
      val hbaseConf = HbaseUtil.getHbaseConf
      val hConnection = ConnectionFactory.createConnection(hbaseConf)

    //      LoggerUtil.info("hbase connet successfully")

      // 读取停用词典
//      val stopWords = sc.textFile("file:///home/sentiment/dicts/stop_words_CN").collect()
      val stopWords = Source.fromFile("/home/sentiment/dicts/stop_words_CN").getLines().toArray
      val stopWordsBr = sc.broadcast(stopWords)

    //      LoggerUtil.info("stopwords read successfully")

      // 初始化词典，存入dicBuffer
//      val dictUser = sc.textFile("file:///home/sentiment/dicts/user_dic.txt").collect()
//      val dictP = sc.textFile("file:///home/sentiment/dicts/posi_dic.txt").collect()
//      val dictN = sc.textFile("file:///home/sentiment/dicts/nega_dic.txt").collect()
//      val dictF = sc.textFile("file:///home/sentiment/dicts/neg_dic.txt").collect()
      val dictUser = Source.fromFile("/home/sentiment/dicts/user_dic.txt").getLines().toArray
      val dictP = Source.fromFile("/home/sentiment/dicts/posi_dic.txt").getLines().toArray
      val dictN = Source.fromFile("/home/sentiment/dicts/nega_dic.txt").getLines().toArray
      val dictF = Source.fromFile("/home/sentiment/dicts/neg_dic.txt").getLines().toArray


    //      LoggerUtil.info("dicts read successfully")

      val dicBuffer = new ArrayBuffer[Array[String]]()
      dicBuffer.append(dictUser)
      dicBuffer.append(dictP)
      dicBuffer.append(dictN)
      dicBuffer.append(dictF)


    //      LoggerUtil.info("dicts read successfully      22222222")

      // 创建表名，根据表名读redis
      val now = new Date()
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val time = dateFormat.format(now)
//      LoggerUtil.info("create redis table name successfully")

      val industryTime = "Industry_" + time                       // ------------------------------- industry -----------------------------
      val stockTime = "Stock_" + time                             // ------------------------------- stock --------------------------------
      val sectionTime = "Section_" + time                         // ------------------------------- section -------------------------------
      val newsTime = "News_" + time                               // -------------------------------- news ---------------------------------

      // 初始化模型
      val models = PredictWithNb.init("/home/sentiment/models/QQ_3300_1500")



    // 计算新闻的倾向比例，写入redis
      val list0 = countPercents(redis, industryTime, newsTime, hConnection, stopWordsBr, dicBuffer,models)

    //      LoggerUtil.info("predict industry trend successfully")
      RedisUtil.writeToRedis(redis, "industry_sentiment", list0)
//      LoggerUtil.info("write industry trend to redis successfully")

      val list1 = countPercents(redis, stockTime, newsTime, hConnection, stopWordsBr, dicBuffer, models)
//      LoggerUtil.info("predict stock trend successfully")
      RedisUtil.writeToRedis(redis, "stock_sentiment", list1)
//      LoggerUtil.info("write stock trend to redis successfully")

      val list2 = countPercents(redis, sectionTime, newsTime, hConnection, stopWordsBr, dicBuffer, models)
//      LoggerUtil.info("predict section trend successfully")
      RedisUtil.writeToRedis(redis, "section_sentiment", list2)
//      LoggerUtil.info("write section trend to redis successfully")

      //      val list = countPercentsRDD(sc, redis, industryTime, newsTime, hbaseConf, stopWords, dicBuffer, args(6))
//      RedisUtil.writeToRedis(redis, "industry_sentiment_RDD", list)

      hConnection.close()
//      LoggerUtil.info("close hbase connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

      redis.close()
//      LoggerUtil.info("close redis connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
//    }catch {
//      case e:Exception =>
//        LoggerUtil.error(e.getMessage)
//    } finally {
      sc.stop()
//      LoggerUtil.info("sc stop >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
//    }

  }


  /**
    * 根据分类信息计算情感倾向的比例
    *
    * @param redis Jedis对象
    * @param classifyTable 数据表名称
    * @param newsTable 新闻数据表名称
    * @param hConnection hbase链接
    * @return 返回（存有类别-比值信息的Map）
    */
  def countPercents(redis:Jedis, classifyTable:String, newsTable:String, hConnection: Connection, stopWordsBr:Broadcast[Array[String]],
                    dicBuffer:ArrayBuffer[Array[String]], model: Map[String, Any]):mutable.Map[String, String] = {

    // 获得所有类别名称
    val s = redis.hkeys(classifyTable)
    val classify = new  Array[String](s.size())
    s.toArray(classify)

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
        val content = HbaseUtil.getValue(hConnection, "wk_detail", newsUrl, "basic", "content")

        // 如果匹配到正文，利用模型预测新闻的情感倾向
        if(content != "Null"){

          //          content = newsTitle + content
          val value = PredictWithNb.predictWithSigle(content, model, stopWordsBr.value)

          if (value == "neg"){
            negaCount = negaCount + 1
          }
          else{
            posiCount = posiCount + 1
          }
        }
        // 如果匹配不到正文，利用词典预测标题的情感倾向
        else{
          val value = SentiRelyDic.searchSenti(newsTitle, dicBuffer)
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

//  /**
//    * 匹配redis中是否包含此条url的信息
//    *
//    * @param url 待匹配的url
//    * @param cateMap  Map[类别名称，Array[（url, title）]}，注意一个类别中问问包含多条新闻
//    * @return url的行业信息，注意一条新闻有时分布于多个不同的类别中
//    * @author liumiao
//    */
//  private def hasUrl(url: String, cateMap: Map[String, Array[(String, String)]]): String = {
//    val result = ArrayBuffer[String]()
//    cateMap.foreach(line => {
//      // 判断当前类别中的所有新闻是否包含指定的url
//      val temp = line._2.map(_._1).contains(url)
//      if (temp){
//        result.append(line._1)
//      }
//    })
//    // 返回String
//    if (result.isEmpty){
//      "there is no cate"
//    } else result.mkString(",")
//  }

//  /**
//    * 读取redis的新闻信息
//    *
//    * @param redis redis 链接信息
//    * @param categoryTable 类别表名
//    * @param newsTable  新闻表名
//    * @return 新闻信息，返回Map[类别名称，Array[（url, title),(url, title),…]}
//    */
//  private def getAllCateNews(redis:Jedis, categoryTable:String, newsTable:String): Map[String, Array[(String, String)]] ={
//    // 获得所有类别的名称（即Key值）
//    val categoryKeys = redis.hkeys(categoryTable).toArray.map(_.asInstanceOf[String])
//    // 根据类别的名称，获得每个类别中所有的新闻ID
//    val cateMap = categoryKeys.map(key => {
//      val newsID = redis.hget(categoryTable, key).split(",")
//      //根据新闻ID，取出每条新闻的“url”和“title”
//      val newsInfo = newsID.map(id => {
//        val newJson = new JSONObject(redis.hget(newsTable, id))
//        val title = newJson.getString("title")
//        val url = newJson.getString("url")
//        (url, title)
//      })
//      // 返回类别和该类别下的所有新闻的title和url
//      (key, newsInfo)
//    }).toMap
//    // Map[类别名称，Array[（url, title),(url, title),…]}
//    cateMap
//  }
}
