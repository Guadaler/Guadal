package com.kunyandata.nlpsuit.util

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * Created by QQ on 2016/3/18.
  *
  */

object TextProcess {

  /**
    * 格式化文本，转化空白字符为停用词表中的标点符号，同时统一英文字母为小写
    * @param content 原始文本字符串
    * @return 返回格式化之后的字符串
    */
  def formatText(content: String): String = {
    val result = content.replaceAll("[ 　\n\t\r\f\b]", "，").toLowerCase
    result
  }

  /**
    * 去除分词结果中的标点符号和停用词
    * @param content 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWords(content: Array[String], stopWords:Array[String]): Array[String] = {
    var result = content.toBuffer
    stopWords.foreach(stopWord => {
      if (result.contains(stopWord)){
        result = result.filterNot(_ == stopWord)
      }
    })
    result.toArray
  }

  /**
    * 调用WordSeq里面的函数实现字符串的分词和去停,并分装成方法
    *
    * @param content 需要处理的字符串
    * @param stopWordsBr 停用词
    * @return 返回分词去停后的结果
    */
  def process(content: String, stopWordsBr: Broadcast[Array[String]]): String = {

    // 格式化文本
    val formatedContent = formatText(content)

    // 实现分词
    val splitWords = WordSeg.splitWord(formatedContent, 0)

//    // 读取分词内容并转化成Array格式
//    val stopWord = WordSeg.getWords(splitWords)
//
//    // 实现去停用词
//    val result = removeStopWords(stopWord, stopWordsBr.value)
    splitWords
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mltest").setMaster("local")
    val sc = new SparkContext(conf)
    val stopWords = Source.fromFile("/home/mlearning/dicts/stop_words_CN").getLines().toArray
    val stopWordsBr = sc.broadcast(stopWords)
    val trainingSet = Source.fromFile("/home/mlearning/trainingSets").getLines().toArray

    // 驱动名称
//    val driver = "com.mysql.jdbc.Driver"
//    //访问本地mysql服务器,通过3306端口
//    // val url = "jdbc:mysql://localhost/mysql"
//    // val url = "jdbc:mysql://localhost:3306/mysql"
//    // val url = "jdbc:mysql://127.0.0.1:3306/mysql"
//    // 访问其他服务器
//    val jdbcUrl = "jdbc:mysql://192.168.1.14:3306/mysql"
//    // 用户名
//    val username = "root"
//    // 密码
//    val password = "root"
//    // 读取sql里面的内容
//    // val sqlString="SELECT * FROM indus_text_with_label WHERE id>3000 and id<=15000"
//    val sqlString = "SELECT url, content FROM stock.indus_text_with_label group by url"
//    // 调用函数获取数据库中的数据
//    val connection = MySQLUtil.getConnect(driver, jdbcUrl, username, password)
//    val result = MySQLUtil.getResult(connection,sqlString)

    // 解析数据库中的数据
    //    var results = new ArrayBuffer[Tuple4[String,String,String,String]]
    //    while ( result.next()) {
    //      val id = result.getString("id").trim
    //      val indus_code = result.getString("indus_code").trim
    //      val title = result.getString("title").trim
    //      val content = result.getString("content").trim
    //      results.append((id, indus_code, title, content, url)
    //    }

    // 定义splitResults 保存分词结果
    val splitResults = new ArrayBuffer[(String, String)]
//    while (result.next()) {
//      val url = result.getString("url").trim
//      val content = result.getString("content").trim.replaceAll("[ 　\f\n\r\b\t]", "，")
////      val segContent = TextProcess.process(content, stopWordsBr)
//      // println(content)
//      // println(wordsList.toSeq)
//      splitResults.append((url, content))
//    }
    // 分词
    trainingSet.foreach(line => {
      val temp = line.split("\t")
      val segResult = WordSeg.splitWord(temp(1), 0)
      println(segResult)
      splitResults.append((temp(0), segResult))
    })

    // 断开连接
//    connection.close()
    // 读取的数据保存到本地txt文件
    val DataFile = new File("/home/mlearning/segTrainingSets")
    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
    splitResults.toArray.foreach(x => {
      bufferWriter.write(x._1 + "\t" + x._2 + "\n")
    })
    bufferWriter.flush()
    bufferWriter.close()
    sc.stop()
  }
}
