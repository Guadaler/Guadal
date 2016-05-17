package numeralCalculations

import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix._
import com.kunyandata.nlpsuit.util.TextPreprocessing
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by QQ on 2016/5/12.
  */
object CosineComputing {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("corrMatrix")
//      .setMaster("local")
    //      .set("spark.local.ip", "192.168.2.90")
//      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)
    val stopWordsBr = sc.broadcast(Source.fromFile(args(1)).getLines().toArray)
    //获取数据
    val data = sc.parallelize(Source.fromFile(args(2)).getLines().toSeq, args(0).toInt).map(line => {
      val temp = line.split("\t")
      if (temp.length == 2)
        temp(1).split(",")
    }).filter(_ != ()).map(line => {
      val temp = line.asInstanceOf[Array[String]]
      TextPreprocessing.removeStopWords(temp, stopWordsBr.value)
    })

    computeCosineByRDD(sc, data).saveAsTextFile(args(3))
    sc.stop()
  }
}
