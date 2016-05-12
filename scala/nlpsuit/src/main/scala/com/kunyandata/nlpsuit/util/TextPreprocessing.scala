package com.kunyandata.nlpsuit.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/3/18.
  * 文本标准化处理过程
  */
object TextPreprocessing {

  /**
    * 格式化文本，转化空白字符为停用词表中的标点符号，同时统一英文字母为小写
    *
    * @param content 原始文本字符串
    * @return 返回格式化之后的字符串
    */
  def formatText(content: String): String = {

    val step = 65248
    val sbcStart = 65281.toChar
    val sbcEnd = 65374.toChar
    val sbcSpace = 12288.toChar
    val dbcSpace = 32.toChar
    val bufferString = new ArrayBuffer[Char]

    if(content == null) {
      content
    } else {
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
    * @param content 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWords(content: Array[String], stopWords:Array[String]): Array[String] = {
    if (content !=null) {
      var result = content.toBuffer
      stopWords.foreach(stopWord => {
        if (result.contains(stopWord)){
          result = result.filterNot(_ == stopWord)
        }
      })
      result.toArray
    } else {
      null
    }
  }

  /**
    * 调用WordSeq里面的函数实现字符串的分词和去停,并封装成方法
    * @param content 需要处理的字符串
    * @param stopWords 停用词
    * @param kunyanConf 坤雁分词模式的设置
    * @return 返回分词去停后的结果
    */
  def process(content: String, stopWords: Array[String], kunyanConf: KunyanConf): Array[String] = {

    // 格式化文本
    val formatedContent = formatText(content)

    // 实现分词
    val splitWords = WordSegment.split(formatedContent, kunyanConf.host, kunyanConf.port)

    // 读取分词内容并转化成Array格式
    val resultWords = splitWords.map(_._1).toArray

    // 实现去停用词
    if (resultWords == null)
      null
    else
      removeStopWords(resultWords, stopWords)
  }

  /**
    * 实现字符串的分词和去停,并分装成方法 ，与上面的process()流程相同，只是分词采用ansj
    * @param content 需要处理的字符串
    * @param stopWords  停用词
    * @return 返回分词去停后的结果
    * @author zhangxin
    */
  def process(content: String, stopWords:Array[String]): Array[String] = {

    // 格式化文本
    val formatedContent = TextPreprocessing.formatText(content)

    // 实现分词
    val resultWords = AnsjAnalyzer.cutNoTag(formatedContent)

    // 实现去停用词
    if (resultWords == null)
      null
    else
      removeStopWords(resultWords, stopWords)
  }

}