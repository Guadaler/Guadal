package numeralCalculations

import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix._
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.{SparkConf, SparkContext}

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
    val kunyanConfigBr = sc.broadcast(new KunyanConf)
    val stopWordsBr = sc.broadcast(Source.fromFile(args(1)).getLines().toArray)
    //获取数据
    val data = sc.parallelize(Source.fromFile(args(2)).getLines().toSeq, args(0).toInt)
      .map(_.split("\t")).filter(_.length == 2)
      .map(line => TextPreprocessing.process(line(1), stopWordsBr.value, kunyanConfigBr.value))

    val result = computeCosineByRDD(sc, data).cache()
    result.saveAsTextFile(args(3))
    result.map(line => (line._1._1, (line._1._2, line._2, line._3, line._4))).groupByKey.map(line => {
      val sorted = line._2.toArray.sortBy(_._4)
      (line._1, sorted)
    }).saveAsTextFile(args(4))
    sc.stop()
  }
}
