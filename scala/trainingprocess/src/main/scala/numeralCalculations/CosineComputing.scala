package numeralCalculations

import java.io.{File, PrintWriter}

import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix._
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.{SparkConf, SparkContext}
import sentiment.JsonConfig

import scala.io.Source

/**
  * Created by QQ on 2016/5/12.
  */
object CosineComputing {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("cosineCorr")
//      .setMaster("local")
    //      .set("spark.local.ip", "192.168.2.90")
//      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)
    val config = new JsonConfig
    config.initConfig(args(2))

    // init paras
    val stopPuncPath = config.getValue("dicts", "stopPuncPath")
    val partition = args(1).toInt
    val cosineTextDataPath = config.getValue("cosineTextData", "cosineTextDataPath")
    val stopWordsBr = sc.broadcast(Source.fromFile(stopPuncPath).getLines().toArray)
    val support = args(0).toInt
    val outputCosine = config.getValue("output", "outputCosine")
    val outputCosineSorted = config.getValue("output", "outputCosineSorted")

    //获取数据
    val data = sc.parallelize(Source.fromFile(cosineTextDataPath).getLines().toSeq, partition)
      .map(_.split("\t")).filter(_.length == 2)
      .map(line => TextPreprocessing.removeStopWords(line(1).split(","), stopWordsBr.value))
    val result = computeCosineByRDD(sc, data, support).cache()
    result.saveAsTextFile(outputCosine + "_" + support)
    result.map(line => (line._1._1, (line._1._2, line._2, line._3, line._4, line._5, line._6))).groupByKey.map(line => {
      val sorted = line._2.toArray.sortWith(_._4 > _._4).take(100)
      (line._1, sorted.mkString(","))
    }).saveAsTextFile(outputCosineSorted + "_" + support)
    val max = result.map(_._4).max()
    val min = result.map(_._4).min()
    val writer = new PrintWriter(new File("/home/mlearning/min_max"))
    writer.write(max + ", " + min)
    writer.flush()
    writer.close()
    sc.stop()
  }
}
