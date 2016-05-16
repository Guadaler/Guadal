package numeralCalculations

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import com.kunyandata.nlpsuit.cluster.SpectralClustering._

import scala.io.Source

/**
  * Created by QQ on 2016/5/12.
  */
object CosineComputing {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SClusterTest")
      .setMaster("local")
    //      .set("spark.local.ip", "192.168.2.90")
//      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

//    System.gc()
//    val total1 = Runtime.getRuntime.totalMemory()
//    val m1 = Runtime.getRuntime.freeMemory()
//    val k1 = total1 - m1
//    println("before: " + k1)

    //获取数据
    val data = sc.parallelize(Source.fromFile(args(1)).getLines().toSeq, args(0).toInt).map(line => {
      val temp = line.split("\t")
      if (temp.length == 2)
        temp(1).split(",")
    }).filter(_ != ()).map(_.asInstanceOf[Array[String]])

//    System.gc()
//    val total2 = Runtime.getRuntime.totalMemory()
//    val m2 = Runtime.getRuntime.freeMemory()
//    val k2 = total2 - m2
//    println("after: " + k2)
//    println("diff: " + (k1 - k2))

    //    val data = sc.parallelize(Seq((0, Array("a", "b", "c", "d")), (1, Array("a", "c", "d", "e")), (2, Array("b", "d", "f", "g", "k")))).cache()
//
    val aRDD = createTermDocMatrix(sc, data, args(0).toInt)
    createCorrRDD(aRDD, args(0).toInt).saveAsTextFile(args(2))
  }

}
