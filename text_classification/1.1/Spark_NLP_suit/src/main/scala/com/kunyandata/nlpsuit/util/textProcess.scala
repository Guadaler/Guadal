package com.kunyandata.nlpsuit.util

import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by QQ on 2016/3/18.
  */

object textProcess extends App{

  case class RawDataRecord(ID: String, category: String ,labels: Double ,text: String)

  val conf = new SparkConf().setMaster("local").setAppName("textProcess")
  val sc = new SparkContext(conf)

  //导入预处理String数据
  val srcRDD = sc.textFile("/users/li/Intellij/Native-Byes/nativebyes/2.txt").map {
    line =>
      val data = line.split(",")
      RawDataRecord(data(0),data(1),labels = if(data(1) == "881155" ) 1.0 else 0.0, data(2))
  }.toString()

  //导入本地停用词
  var stopWord = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/stop_words_CN" )
  var stopWords = stopWord.getLines().toArray

  // 调入分词系统
  val context = WordSeg.splitWord(srcRDD,0).toCharArray

  val result = WordSeg.removeStopWords(context,stopWords)
  sc.stop()


}
