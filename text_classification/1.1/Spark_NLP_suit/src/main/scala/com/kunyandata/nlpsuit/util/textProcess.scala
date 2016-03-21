package com.kunyandata.nlpsuit.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by QQ on 2016/3/18.
  */

object textProcess extends App{

  //  var resultMap: Map[String, Any] = Map()

  val conf = new SparkConf().setMaster("local").setAppName("textProcess")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  //导入预处理String数据
  val src = "9日上午，“交银国信·周浦花海土地承包经营权流转单一信托”成立签约仪式在上海举行。"

  //导入本地停用词
  var stopWord = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/stop_words_CN" )
  var stopWords = stopWord.getLines().toArray

  //调用分词系统
    val context = WordSeg.splitWord(src ,1)
    val contest = WordSeg.getWords(context)
    val fited = WordSeg.removeStopWords(contest,stopWords)

  fited.foreach(println)


  sc.stop()


}
