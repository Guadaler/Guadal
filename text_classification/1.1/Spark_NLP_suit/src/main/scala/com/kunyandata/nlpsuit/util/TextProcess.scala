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
    * 调用WordSeq里面的函数实现字符串的分词和去停,并分装成方法
    *
    * @param context     需要处理的字符串
    * @param stopWordsBr 停用词
    * @return 返回分词去停后的结果
    */
  def process(context: String, stopWordsBr: Broadcast[Array[String]]): Array[String] = {

    //实现分词
    val splitWords = WordSeg.splitWord(context, 1)

    //读取分词内容并转化成Array格式
    val stopWord = WordSeg.getWords(splitWords)

    //实现去停用词
    val result = WordSeg.removeStopWords(stopWord, stopWordsBr.value)
    result
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mltest").setMaster("local")
    val sc = new SparkContext(conf)
    val stopWords = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/stop_words_CN").getLines().toArray
    val stopWordsBr = sc.broadcast(stopWords)

    // 驱动名称
    val driver = "com.mysql.jdbc.Driver"
    //访问本地mysql服务器,通过3306端口
    // val url = "jdbc:mysql://localhost/mysql"
    // val url = "jdbc:mysql://localhost:3306/mysql"
    // val url = "jdbc:mysql://127.0.0.1:3306/mysql"
    // 访问其他服务器
    val jdbcUrl = "jdbc:mysql://192.168.1.14:3306/mysql"
    // 用户名
    val username = "root"
    // 密码
    val password = "root"
    // 读取sql里面的内容
    // val sqlString="SELECT * FROM indus_text_with_label WHERE id>3000 and id<=15000"
    val sqlString = "SELECT url, content FROM stock.indus_text_with_label group by url"
    // 调用GetResultSet函数获取数据库中的数据
    val result = MySQLUtil.GetResultSet(driver, jdbcUrl, username, password, sqlString)

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
    val splitResults = new ArrayBuffer[(String, Array[String])]
    while (result.next()) {
      val url = result.getString("url").trim
      val content = result.getString("content").trim
      val wordsList = TextProcess.process(content, stopWordsBr)
      splitResults.append((url, wordsList))
    }



    // 读取的数据保存到本地txt文件
    //    print(splitResults)

    val splitResult = splitResults.toArray
    // 读取的数据保存到本地txt文件
    val DataFile = new File("out.txt")
    val bw = new BufferedWriter(new FileWriter(DataFile))
    splitResult.foreach(x => {
      bw.write(x._1 + "," + x._2 + "\n")
    })
    bw.flush()
    bw.close()


  }
}
