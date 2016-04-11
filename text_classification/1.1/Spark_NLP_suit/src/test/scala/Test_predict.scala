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
    val content = "原标题：国家发改委：中国足球2030年前跻身世界强队\n"
    val result =PredictWithNb.predictWithSigle(content, model, stopWordsBr)
    println(result)
  }
}
