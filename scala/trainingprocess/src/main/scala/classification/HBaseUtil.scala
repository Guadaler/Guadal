package classification

import java.text.SimpleDateFormat
import java.util.Date

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import sentiment.SentimentConf

/**
  * Created by liumiao on 2016/4/28.
  * hbase操作
  */
object HBaseUtil {

  /**
    * 连接 hbase
    *
    * @param sentimentConf 配置文件
    * @return hbase信息
    * @author liumaio
    */
  def getHbaseConf(sentimentConf: SentimentConf): Configuration = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.rootdir", sentimentConf.getValue("hbase", "rootDir"))
    hbaseConf.set("hbase.zookeeper.quorum", sentimentConf.getValue("hbase", "ip"))

    hbaseConf
  }

  /**
    * 识别字符编码
    *
    * @param html 地址编码
    * @return 字符编码
    * @author wc
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
    * @param sc Spark程序入口
    * @param hbaseConf hBase信息
    * @return RDD[url，标题，正文]
    * @author wc
    */
  def getRDD(sc: SparkContext, hbaseConf: Configuration): RDD[String] = {

    val tableName = "wk_detail"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
//    hbaseConf.set(TableInputFormat.SCAN, setTimeRange())

    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])

    val news = hbaseRdd.map( x => {

      val a = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val b = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val c = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      val aFormat = judgeChaser(a)
      val bFormat = judgeChaser(b)
      val cFormat = judgeChaser(c)
      new String(a, aFormat) + "\n\t" + new String(b, bFormat) + "\n\t" + new String(c, cFormat)

    }).cache()

    news
  }

  /**
    * 设置时间范围
    *
    * @return 时间范围
    * @author yangshuai
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
