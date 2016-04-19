package com.kunyandata.nlpsuit.sentiment

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlp.util.{HbaseUtil, RedisUtil}
import com.kunyandata.nlpsuit.util.RedisUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Table}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable.Map
import scala.io.Source

/**
  * Created by Liu on 2016/4/13.
  */

object News_trend_pre {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("News_trend_pre")
      .setMaster("local")
    val sc = new SparkContext(conf)

    try {

      val redis = RedisUtil.get_redis(sc, "E:\\info_txt\\redis_info.txt")

      val hbaseConf = HbaseUtil.getHbaseConf()

      val stopWords = Source.fromFile("E:\\senti_learn\\text_classification\\1.1\\Spark_NLP_suit\\src\\main\\resources\\dicts\\stop_words_CN").getLines().toArray  //读取停用词典并转成Array
      val stopWordsBr = sc.broadcast(stopWords)

      // get all table's name
      val now = new Date()
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val time = dateFormat.format(now)

      //    val time = "20160129"
      val ind_time = "Industry_" + time                        // ------------------------------- industry -----------------------------
      val sto_time = "Stock_" + time                           // ------------------------------- stock --------------------------------
      val sec_time = "Section_" + time                        // ------------------------------- section -------------------------------
      val key_time = "News_" + time                           // -------------------------------- news ---------------------------------

      val list0 = count_percents(sc, redis, ind_time, key_time, hbaseConf, stopWordsBr)
      val list1 = count_percents(sc, redis, sto_time, key_time, hbaseConf, stopWordsBr)
      val list2 = count_percents(sc, redis, sec_time, key_time, hbaseConf, stopWordsBr)

      RedisUtil.write_To_Redis(redis, "industry_sentiment", list0)
      RedisUtil.write_To_Redis(redis, "stock_sentiment", list1)
      RedisUtil.write_To_Redis(redis, "section_sentiment", list2)

      redis.close()
      println("close redis connection>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    }catch {
      case e:Exception =>
        println(e.getMessage)
    } finally {
      sc.stop()
      println("sc stop >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }

  }


  /**
    * 根据分类信息计算情感倾向的比例
    *
    * @param redis Jedis对象
    * @param db_name 数据表名称
    * @param db_news 新闻数据表名称
    * @param hbaseconf hbase
    * @return 返回（存有类别-比值信息的Map）
    */
  def count_percents(sc:SparkContext, redis:Jedis, db_name:String, db_news:String, hbaseconf:Configuration,stopWordsBr:Broadcast[Array[String]]):Map[String, String] = {
    // get all classify information and news' code
    val s = redis.hkeys(db_name)                           // get classify name
    val c = redis.hkeys(db_news)                           // get all news code
    val classify = new  Array[String](s.size())
    val code = new Array[String](c.size())
    s.toArray(classify)
    c.toArray(code)

    //初始化 -> Byes -> initModel 文件路径 （目前是绝对路径）
    val model = PredictWithNb.init()

    // create a Map to store result
    val result = Map[String, String]()

    // 本地存储（文件）
    val now = new Date()
    val dateFormat = new SimpleDateFormat("HHmmss")
    //    val writer = new PrintWriter(new File(db_name + "_" +dateFormat.format(now) + ".txt" ))
    val writer = new PrintWriter(new File("E:\\text\\feach_news_content.txt"))

    // for every industry count news tendency percent
    for (i <- Range(0,classify.length)) {
      var n = 0
      var p_m = 0
      var sum = 0.0f
      var conse = ""
      // get every industry's all news code
      val ss = redis.hget(db_name, classify(i))
      // print classify name
      //      writer.write(classify(i) + "\n")
      val news = ss.split(",")

      // for every new get it's title info
      for (j <- Range(0, news.length)) {
        //        if(code.contains(news(j))){
        val all = redis.hget(db_news, news(j))
        // get title
        val tt = new JSONObject(all)
        //        val t = tt.getString("title")
        val t = tt.getString("url")

        //用“url”=> t 读出新闻的正文
        val content = HbaseUtil.getValue(hbaseconf, "wk_detail", t, "basic", "content")

        if(content != "Null"){
          // 调用函数 获得正文的情感倾向
//          println(content)
          val value = PredictWithNb.predictWithSigle(content, model, stopWordsBr)
//          println(value )
          if (value == "neg"){
            n = n + 1
          }
          else{
            p_m = p_m + 1
          }
        }
        //如果匹配不到新闻
        else{
          val title_cut = Title_senti_dic.cut(sc, t)
          val value = Title_senti_dic.search_senti(sc, title_cut)
          if (value < 0) {
            n = n + 1
          }
          else {
            p_m = p_m + 1
          }
        }

      }
      sum = n + p_m

      println(classify(i) + " " + n + " " + p_m)

//      val jsoninfo = RedisUtil.toJSON( classify(i), n, p_m, sum)

      conse = (n/sum).toString + "," + (p_m/sum).toString

      result += (classify(i) -> conse)

    }
    writer.close()
    result
  }

}
