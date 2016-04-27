package com.kunyan.util

import java.text.SimpleDateFormat
import java.util.Date

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Liu on 2016/4/13.
  */

object HbaseUtil {

  /**
    * 连接 hbase
    *
    * @return 返回hbaseConf资源
    * @author liumaio
    */
  def getHbaseConf: Configuration = {
    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.rootdir", "hdfs://222.73.57.12/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "222.73.57.12,222.73.57.3,222.73.57.7")

//    hbaseConf.set("hbase.rootdir", "hdfs://222.73.34.99:9000/hbase")
//    hbaseConf.set("hbase.zookeeper.quorum", "222.73.34.95,222.73.34.96,222.73.34.99")

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
    hbaseConf.set(TableInputFormat.SCAN, setTimeRange)
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
    * @param hConnection hbase链接
    * @param tablename 需要读取的表名
    * @param rowkey 键值
    * @param family 列簇
    * @param colume 列名
    * @return value值
    * @author liumiao
    */
  def getValue(hConnection:Connection, tablename:String, rowkey:String, family:String, colume:String): String = {
    //tablename：表名
    val table = hConnection.getTable(TableName.valueOf(tablename))
    //rowkey：hbase的rowkey
    val get = new Get(rowkey.getBytes())
    val result = table.get(get)
    //family：hbase列族  column：hbase列名
    val data = result.getValue(family.getBytes, colume.getBytes)
    // 返回读出的值
    if(data == null)
      "Null"
    else
      new String(data, judgeCharser(data))
  }

  private def setTimeRange(): String = {

    val scan = new Scan()
    val date = new Date(new Date().getTime - 24 * 60 * 60 * 1000)
    val format = new SimpleDateFormat("yyyy-MM-dd HH")
    val time = format.format(date)
    val time1 = format.format(new Date().getTime)
    val startTime = time + "-00-00"
    val stopTime = time1 + "-00-00"
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    val startRow: Long = sdf.parse(startTime).getTime
    val stopRow: Long = sdf.parse(stopTime).getTime

    scan.setTimeRange(startRow, stopRow)
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
}
