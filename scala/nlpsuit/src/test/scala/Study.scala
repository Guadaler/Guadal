import breeze.linalg.DenseVector
import com.kunyandata.nlpsuit.Statistic.Similarity
import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix._
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by QQ on 2016/5/14.
  */
object Study {

  def myfunc(index: Int, iter: Iterator[(Long, Seq[String])]) : Iterator[String] = {
    iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("study")
      .setMaster("local")
      .set("spark.driver.host", "192.168.2.90")


    val sc = new SparkContext(conf)

    val k = 0.005
    val cosineDis = Math.round(Math.abs(Math.log(k)))
    val tanCosDis = Math.round(Math.tan(Math.abs(Math.log(k)) * Math.PI / 2))
    println(cosineDis)
    println(Math.log(k))
    println(tanCosDis)
  }
}
