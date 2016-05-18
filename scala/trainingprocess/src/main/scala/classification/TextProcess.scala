package classification

import org.apache.spark.{SparkConf, SparkContext}
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import sentiment.SentimentConf

/**
  * Created by QQ on 4/19/16.
  */
object TextProcess {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordSegmentation")
//      .setMaster("local")
//      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

    val config = new SentimentConf
    config.initConfig(args(0))
    val kunyanConfig = new KunyanConf
    val kunyanHost = config.getValue("kunyan", "host")
    val kunyanPort = config.getValue("kunyan", "port").toInt
    kunyanConfig.set(kunyanHost, kunyanPort)

    val hbaseConf = HBaseUtil.getHbaseConf(config)
    val hbaseAllNews = HBaseUtil.getRDD(sc, hbaseConf)
      .repartition(config.getValue("RDD", "partition").toInt).map(content => {
      content.split("\n\t")
    }).filter(_.length == 3).map(_(2)).cache()

    val output = config.getValue("output", "resultPath")
    val kunyanConfigBr = sc.broadcast(kunyanConfig)
    hbaseAllNews.map(TextPreprocessing.process(_, Array(""), kunyanConfigBr.value).mkString(",")).saveAsTextFile(output)
    sc.stop()
  }
}
