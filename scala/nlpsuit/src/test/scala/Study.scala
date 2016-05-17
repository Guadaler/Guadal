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

    val rdd = sc.parallelize(Seq((1L, Array("a", "b", "c", "f")),
      (2L, Array("b", "c", "d", "e")), (3L, Array("c", "d", "e", "f"))))

    computeCosineByRDD(sc, rdd.values).foreach(println)

    val kkk = createTermDocMatrix(sc, rdd, 2)
    val zzz = createCorrRDD(kkk, 2)
    zzz.filter(_._1 == 2L).foreach(line => println(line._1, line._2.toSeq))

  }
}
