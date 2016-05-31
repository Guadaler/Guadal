import breeze.linalg.DenseVector
import com.kunyandata.nlpsuit.Statistic.Similarity
import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix._
import com.kunyandata.nlpsuit.util._
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
//      .set("spark.driver.host", "192.168.2.90")
    val config = new KunyanConf()
    config.set("222.73.57.17", 16003)
    println(TextPreprocessing.process("123", Array(""), config).toSeq)
  }
}
