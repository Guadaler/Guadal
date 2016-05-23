package com.kunyan.sentiment

import com.kunyan.util.{JsonConfig, LoggerUtil}
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.{TextPreprocessing, KunyanConf}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by Administrator on 2016/5/19.
  */
object test {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)

    // 获取配置文件信息
    val configInfo = new JsonConfig()
    configInfo.initConfig(args(0))

    // 初始化分类模型
    val models = PredictWithNb.init(configInfo.getValue("models", "sentModelsPath"))
    val modelsBr = sc.broadcast(models)

    // 配置kunyan分词
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)
    val confBr = sc.broadcast(kunyanConfig)

    val str = sc.parallelize(Source.fromFile(args(1)).getLines().toSeq.take(1000)).repartition(16)

    val stopword = Array[String]("," , "-")
    val stopBr = sc.broadcast(stopword)

    val results = str.map( s => {

      val cut = TextPreprocessing.process(s, stopword, kunyanConfig)
      var ss = ""
      cut.foreach( c => {
        ss += c + "  "
      })

      ss
    }).collect()

    results.foreach(s => {
      LoggerUtil.warn( s  +  " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    })

    sc.stop()
    LoggerUtil.warn("sc stop >>>>>>>>>>>>>>>>>>>>>>")
  }

}
