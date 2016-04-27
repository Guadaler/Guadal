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
import org.apache.spark.SparkContext

/**
  * Created by Liu on 2016/4/13.
  */

object HBaseUtil {

  /**
    * 连接 hbase
    *
    * @return 返回hBaseConf资源
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
  def judgeChaser(html: Array[Byte]): String = {
    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()
    encoding.getName
  }

  /**
    * 读取内容信息
    *
    * @param sc SparkContext
    * @param hBaseConf hBase资源
    * @return RDD
    */
  def getRDD(sc:SparkContext, hBaseConf:Configuration): RDD[String] ={
    //表名
    val tableName = "wk_detail"
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hBaseConf.set(TableInputFormat.SCAN, setTimeRange())
    //获得RDD
    val hBaseRdd = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])
    //获得url、title、content列
    val news = hBaseRdd.map( x => {
      val a = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val b = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val c = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      //编码转换
      val formatA = judgeChaser(a)
      val formatB = judgeChaser(b)
      val formatC = judgeChaser(c)
      new String(a, formatA) + "\n\t" + new String(b, formatB) + "\n\t" + new String(c, formatC)
    })
    //返回RDD
    news
  }

  /**
    * 读 hBase 中的表
    *
    * @param hConnection hBase链接
    * @param tableName 需要读取的表名
    * @param rowKey 键值
    * @param family 列簇
    * @param column 列名
    * @return value值
    * @author liumiao
    */
  def getValue(hConnection:Connection, tableName:String, rowKey:String, family:String, column:String): String = {
    //tableName：表名
    val table = hConnection.getTable(TableName.valueOf(tableName))
    //rowKey：hBase的rowKey
    val get = new Get(rowKey.getBytes())
    val result = table.get(get)
    //family：hBase列族  column：hBase列名
    val data = result.getValue(family.getBytes, column.getBytes)
    // 返回读出的值
    if(data == null)
      "Null"
    else
      new String(data, judgeChaser(data))
  }

  /**
    * 设置时间范围
    *
    * @return 时间范围
    */
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
