package com.kunyan.util

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Liu on 2016/4/13.
  */

object HbaseUtil {

//  def main(args: Array[String]) {
//
//    val sparkConf = new SparkConf().setMaster("local").setAppName("HbaseUtil")
//    val sparkContext = new SparkContext(sparkConf)
//
//    try{
//      val hbaseConf = getHbaseConf()
//      val news = getRDD(sparkContext, hbaseConf)//.cache()
//
//      news.take(5).foreach(println)
//
//      val newss = news.map( x => {
//        val s = x.split("\n\t")
//        println(s(0))
//        s(0)
//      })
//      newss.take(10).foreach(println)
////      newss.saveAsTextFile("E:\\text\\news_url_20160414.txt")
//
//
////      println(news.count())
////      val newss = news.filter( x => {
////        val ss = x.split("\n\t")
////        ss(0) == "[B@1a4ee87b"
////      })
////      val newsss = news.filter( x => {
////        val ss = x.split("\n\t")
////        ss(0) == "[B@2263811f"
////      })
////      println(newss.count() + "  " + newsss.count())
//
//
////      var content = ""
////      newss.take(1).foreach( x => {
////        val ss = x.split("\n\t")
////        content =ss(2)
////      })
////      println(content)
//
//
////      val data = getValue(hbaseConf, "wk_detail", "http://news.hexun.com/2016-03-23/182919956.html", "basic", "content" )
////      println(data)
//
//    }catch {
//      case e:Exception =>
//        println(e.getMessage)
//    } finally {
//      sparkContext.stop()
//      //      println("sparkContext stop >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
//    }
//  }

  /**
    * 连接 hbase
    *
    * @return 返回hbaseConf资源
    * @author liumaio
    */
  def getHbaseConf(): Configuration = {
    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set("hbase.rootdir", "hdfs://222.73.34.99/hbase")
//    hbaseConf.set("hbase.zookeeper.quorum", "server0,server1,server2,server3,server4")
    hbaseConf.set("hbase.rootdir", "hdfs://222.73.34.99:9000/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "222.73.34.95,222.73.34.96,222.73.34.99")
    hbaseConf
  }

  /**
    * 识别字符编码
    *
    * @param html 地址编码
    * @return
    */
  def judgeCharser(html: Array[Byte]): String = {
    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()
    encoding.getName
  }

  /**
    * 读取内容信息
    *
    * @param sc
    * @param hbaseConf hbase资源
    * @return RDD
    */
  def getRDD(sc:SparkContext, hbaseConf:Configuration): RDD[String] ={
    //表名
    val tableName = "wk_detail"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    //获得RDD
    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])
    //获得url、title、content列
    val news = hbaseRdd.map( x => {
      val a = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val b = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val c = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      //编码转换
      val formata = judgeCharser(a)
      val formatb = judgeCharser(b)
      val formatc = judgeCharser(c)
      new String(a, formata) + "\n\t" + new String(b, formatb) + "\n\t" + new String(c, formatc)
    })
    //返回RDD
    news
  }

  /**
    * 读 hbase 中的表
    *
    * @param hbaseConf hbase资源
    * @param tablename 需要读取的表名
    * @param rowkey 键值
    * @param family 列簇
    * @param colume 列名
    * @return value值
    * @author liumiao
    */
  def getValue(hbaseConf:Configuration, tablename:String, rowkey:String, family:String, colume:String): String = {
    //tablename：表名
    val table = ConnectionFactory.createConnection(hbaseConf).getTable(TableName.valueOf(tablename))

    //rowkey：hbase的rowkey
    val get = new Get(rowkey.getBytes())
    val result = table.get(get)

    //family：hbase列族  column：hbase列名
    val data = result.getValue(family.getBytes, colume.getBytes)

    if(data == null)
      "Null"
    else
      new String(data, judgeCharser(data))
  }

}
