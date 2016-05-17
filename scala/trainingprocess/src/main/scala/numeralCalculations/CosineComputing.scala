package numeralCalculations

import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix._
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

    //获取数据
    val data = sc.parallelize(Source.fromFile(args(1)).getLines().toSeq, args(0).toInt).map(line => {
      val temp = line.split("\t")
      if (temp.length == 2)
        temp(1).split(",")
    }).filter(_ != ()).map(_.asInstanceOf[Array[String]])

    computeCosineByRDD(sc, data).saveAsTextFile(args(2))
    sc.stop()
  }
}
