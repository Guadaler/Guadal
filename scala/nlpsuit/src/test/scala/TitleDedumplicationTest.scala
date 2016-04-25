/**
  * Created by QQ on 2016/2/19.
  */


import java.util.Date

import com.kunyandata.nlpsuit.deduplication.TitleDeduplication
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QQ on 2016/2/21.
  */
object TitleDedumplicationTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("test123")
      .setMaster("local")
      .set("spark.local.ip","192.168.2.65")
      .set("spark.driver.host","192.168.2.65")
    //    .setMaster("spark://222.73.57.12:7077")
    //      .set("spark.local.ip","222.73.57.12")
    //      .set("spark.driver.host","222.73.57.12")
    //    .set("spark.executor.memory", "15G")
    //    .set("spark.executor.cores", "4")
    //    .set("spark.cores.max", "8")

    val sc = new SparkContext(conf)
    val n = 2
    val title1 = "远望谷涨停 将独家供应迪士尼梦想护照"
    val title2 = "远望谷涨停 将独家供应迪士尼梦想护照"
    var titleRDD = sc.makeRDD(Seq(title2))
    var i = 1
    while (i <= 14){
      titleRDD = titleRDD.++(titleRDD)
      i = i + 1
    }
    val titleArray = titleRDD.collect()
    val time1 = new Date().getTime
    val result = TitleDeduplication.process(title1, titleArray, n, 0.4)
    val time2 = new Date().getTime
    println(titleRDD.count())
    println(result)
    println(time2 - time1)
  }

}
