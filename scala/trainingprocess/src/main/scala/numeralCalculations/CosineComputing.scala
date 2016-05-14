package numeralCalculations

import org.apache.spark.{SparkContext, SparkConf}
import com.kunyandata.nlpsuit.cluster.SpectralClustering._
import org.apache.spark.mllib.stat.Statistics
import scala.io.Source

/**
  * Created by QQ on 2016/5/12.
  */
object CosineComputing {


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SClusterTest")
//      .setMaster("local")
    //      .set("spark.local.ip", "192.168.2.90")
    //      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

    //    ++++++++++++++++++++++++++++++++++++++ 计算 adjacency matrix ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //获取数据
    var id = -1
    val data = sc.textFile(args(1), args(0).toInt).map(line => {
        val temp = line.split("\t")
        if (temp.length == 2) {
          id += 1
          (id, temp(1).split(","))
        }
      }).filter(_ != ()).map(_.asInstanceOf[(Int, Array[String])])

    //    val data = sc.parallelize(Seq((0, Array("a", "b", "c", "d")), (1, Array("a", "c", "d", "e")), (2, Array("b", "d", "f", "g", "k")))).cache()

    val wordList = data.map(line => line._2).flatMap(_.toSeq).distinct().collect().sorted
    val wordListBr = sc.broadcast(wordList)
    val aRDD = createDocTermRDD(data, wordListBr)
    val bRDD = createCorrRDD(sc, aRDD.map(_._2), wordListBr)
    bRDD.saveAsTextFile(args(2))
  }

}
