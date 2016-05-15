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
    val data = sc.parallelize(Source.fromFile(args(1)).getLines().toSeq, args(0).toInt).map(line => {
      val temp = line.split("\t")
      if (temp.length == 2)
        temp(1).split(",")
    }).filter(_ != ()).map(_.asInstanceOf[Array[String]])

    //    val data = sc.parallelize(Seq((0, Array("a", "b", "c", "d")), (1, Array("a", "c", "d", "e")), (2, Array("b", "d", "f", "g", "k")))).cache()

    val aRDD = createTermDocMatrix(sc, data, args(0).toInt)
    val bRDD = createCorrRDD(aRDD, args(0).toInt)
    bRDD.saveAsTextFile(args(2))
  }

}
