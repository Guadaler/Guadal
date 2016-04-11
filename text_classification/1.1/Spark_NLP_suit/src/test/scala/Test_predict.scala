//import com.kunyandata.nlpsuit.sentiment.{TextPre_KunAnalyzer, Analyzer, Util}
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.{TextProcess, WordSeg}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source
import scala.util.Random
import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2016/3/29.
  */
object Test_predict {
  def main(args: Array[String]) {
    val sconf=new SparkConf().setAppName("test").setMaster("local")
    val sc=new SparkContext(sconf)

    val stopWords=Source.fromFile("D:\\111_DATA\\data\\stop_words_CN").getLines().toArray  //读取停用词典并转成Array
    val stopWordsBr = sc.broadcast(stopWords)

    //单模型+单篇文章
    val model=PredictWithNb.init()
    val content = "宽带中国”战略提速 设备商大块朵颖四千亿蛋糕"
    val result =PredictWithNb.predictWithSigle(content, model, stopWordsBr)
    println(result)


    //测试RDD
    val a = sc.parallelize(1 to 9, 3)
    println(a)



  }
}
