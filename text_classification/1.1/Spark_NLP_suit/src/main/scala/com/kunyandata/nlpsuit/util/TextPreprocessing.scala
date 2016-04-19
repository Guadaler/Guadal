package com.kunyandata.nlpsuit.util

import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by QQ on 2016/3/18.
  *
  */

import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer

object TextPreprocessing {

//  def copyFile(localPath: String, targetPath: String): Unit = {
//
//    val fis = new FileInputStream(localPath)
//    val bufis = new BufferedInputStream(fis)
//
//    val fos = new FileOutputStream(targetPath)
//    val bufos = new BufferedOutputStream(fos)
//
//    var len = bufis.read()
//    while (len != -1){
//      bufos.write(len)
//      len = bufis.read()
//    }
//
//    bufis.close()
//    bufos.close()
//  }

  /**
    * 格式化文本，转化空白字符为停用词表中的标点符号，同时统一英文字母为小写
    *
    * @param content 原始文本字符串
    * @return 返回格式化之后的字符串
    */
  def formatText(content: String): String = {


    val step = 65248
    val dbcStart = 33.toChar
    val dbcEnd = 126.toChar
    val sbcStart = 65281.toChar
    val sbcEnd = 65374.toChar
    val sbcSpace = 12288.toChar
    val dbcSpace = 32.toChar
    val bufferString = new ArrayBuffer[Char]
    if(content == null){
      content
    }else{
      content
        .replaceAll("<[^<]*>", "")
        .replaceAll("&nbsp", "")
        .foreach(ch => {
        if (ch == sbcSpace){
          bufferString.append(dbcSpace)
        }else if (ch >= sbcStart && ch <= sbcEnd){
          bufferString.append((ch - step).toChar)
        }else{
          bufferString.append(ch)
        }
      })
      bufferString
        .mkString
        .replaceAll("""\s""", "")
        .replaceAll("\"", ",")
    }
  }

  /**
    * 去除分词结果中的标点符号和停用词
    *
    * @param content 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWords(content: Array[String], stopWords:Array[String]): Array[String] = {
    if(content !=null){
      var result = content.toBuffer
      stopWords.foreach(stopWord => {
        if (result.contains(stopWord)){
          result = result.filterNot(_ == stopWord)
        }
      })
      result.toArray
    }else{
      null
    }
  }

  /**
    * 调用WordSeq里面的函数实现字符串的分词和去停,并封装成方法
    *
    * @param content 需要处理的字符串
    * @param stopWordsBr 停用词
    * @param kunyanSegTyp 分词模式选择，0为调用本地分词工具（只支持linux下运行），1为远程调用，过长的文章可能报错。
    * @return 返回分词去停后的结果
    */
  def process(content: String, stopWordsBr: Broadcast[Array[String]], kunyanSegTyp: Int): Array[String] = {

    // 格式化文本
    val formatedContent = formatText(content)
    // 实现分词
    val splitWords = WordSeg.splitWord(formatedContent, kunyanSegTyp)
    // 读取分词内容并转化成Array格式
    val resultWords = WordSeg.getWords(splitWords)
    // 实现去停用词
    if (resultWords == null) null
    else removeStopWords(resultWords, stopWordsBr.value)
  }

  /**
    * 实现字符串的分词和去停,并分装成方法  ，与上面的process()方法相同，只是分词采用ansj
    *
    * @param content 需要处理的字符串
    * @param stopWordsBr  停用词
    * @return 返回分词去停后的结果
    * @author zhangxin
    */
  def process(content: String, stopWordsBr: Broadcast[Array[String]]): Array[String] = {
    // 格式化文本
    val formatedContent =TextPreprocessing.formatText(content)
    // 实现分词
    val resultWords=AnsjAnalyzer.cut(content)
    // 实现去停用词
    if (resultWords == null) null
    else removeStopWords(resultWords, stopWordsBr.value)
  }
}