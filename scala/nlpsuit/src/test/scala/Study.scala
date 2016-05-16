import breeze.linalg.DenseVector
import com.kunyandata.nlpsuit.Statistic.Similarity
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

    val sc = new SparkContext(conf)

    val a = sc.parallelize(Seq(Array(1.0, 1.0, 1.0, 5.0).toSeq, Array(321.5, 32.5, 6.7, 4329.0).toSeq, Array(11.9, 14.5, 16.8, 19.2).toSeq))
    val indexa = a.zipWithIndex()
    val b = indexa.cartesian(indexa)
    b.foreach(println)
    val result = b.map(line => {
      val id1 = line._1._2
      val id2 = line._2._2
      val x = line._1._1
      val y = line._2._1
//      val x = DenseVector(line._1._1.toArray)
//      val y = DenseVector(line._2._1.toArray)
      println(x, y)
      val cosDist = Similarity.cosineDistance(x, y)
      println(cosDist)
      println()

      (id1, id2, cosDist)
    })
    result.foreach(println)
    result.groupBy(_._1).foreach(println)

    val xxx = Array("123", "321", "932", "0")
    val temp = new Array[Int](10)
    println(xxx.count(_ == "1234") + "-----------+++++++++++++++++")
    temp.update(xxx.indexOf("0"), 1)
    println(temp.toSeq)

    val data = sc.parallelize(Source.fromFile(args(1)).getLines().toSeq, args(0).toInt).map(line => {
      val temp = line.split("\t")
      if (temp.length == 2) {
        temp(1).split(",")
      }
    }).filter(_ != ()).map(_.asInstanceOf[Array[String]].toSeq).zipWithIndex().map(k => (k._2, k._1))
    data.mapPartitionsWithIndex(myfunc).collect().foreach(println)

  }

}
