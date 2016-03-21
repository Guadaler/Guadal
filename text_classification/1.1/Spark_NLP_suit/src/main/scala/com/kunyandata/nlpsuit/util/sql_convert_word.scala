package com.kunyandata.nlpsuit.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by li on 16/3/21.
  */
object sql_convert_word extends App{
  case class RawDataRecord(ID: String, category: String ,labels: Double ,text: String)
  //  var resultMap: Map[String, Any] = Map()

  val conf = new SparkConf().setMaster("local").setAppName("textProcess")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  //导入预处理String数据
  val srcRF = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/1.txt")
  val srcdata = srcRF.getLines().toArray

  //  val src = "9日上午，“交银国信·周浦花海土地承包经营权流转单一信托”成立签约仪式在上海举行。据信托计划受托人交银国信介绍，这是上海市的首单土地流转信托尝试"

  //导入本地停用词
  var stopWord = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/stop_words_CN" )
  var stopWords = stopWord.getLines().toArray

  //数据预处理
  val src =  srcdata.map{
    line =>
      var data  = line.split(",")
      RawDataRecord(data(0), data(1), labels = if (data(1)=="881155") 1.0 else 0.0, data(2))
  }

  //调用分词系统
  val result = src.map(line => {
    val context = WordSeg.splitWord(line.text ,1)
    val contest = WordSeg.getWords(context)
    val fited = WordSeg.removeStopWords(contest,stopWords)
    //    RawDataRecord(line.ID, l

    fited
//    RawDataRecord(line.ID,line.category,line.labels,fited.toString)
    //    println(v.text)
  })

  result.foreach(println)



  sc.stop()

}
