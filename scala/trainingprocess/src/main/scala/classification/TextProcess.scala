package classification

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import sentiment.JsonConfig

import scala.io.Source

/**
  * Created by QQ on 4/19/16.
  */
object TextProcess {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordSegmentation")
//      .setMaster("local")
//      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

    // 初始化配置文件
    val config = new JsonConfig
    config.initConfig(args(0))

    // 初始化坤雁分词器的配置文件
    val kunyanConfig = new KunyanConf
    val kunyanHost = config.getValue("kunyan", "host")
    val kunyanPort = config.getValue("kunyan", "port").toInt
    kunyanConfig.set(kunyanHost, kunyanPort)

    val stopPuncBr = sc.broadcast(Source.fromFile(config.getValue("dicts", "stopPuncPath")).getLines().toArray)
    val hbaseConf = HBaseUtil.getHbaseConf(config)
    val hbaseAllNews = HBaseUtil.getRDD(sc, hbaseConf)
      .repartition(config.getValue("RDD", "partition").toInt)
      .map(_.split("\n\t")).filter(_.length == 3).map(_(2)).cache()

    val output = config.getValue("output", "resultPath")
    val kunyanConfigBr = sc.broadcast(kunyanConfig)
    hbaseAllNews.map(TextPreprocessing.process(_, stopPuncBr.value, kunyanConfigBr.value).mkString(",")).saveAsTextFile(output)
    val writer = new PrintWriter(new File("/home/mlearning/hbaseTextCount"))
    writer.write(hbaseAllNews.count().toString)
    writer.flush()
    writer.close()
    sc.stop()
  }
}
