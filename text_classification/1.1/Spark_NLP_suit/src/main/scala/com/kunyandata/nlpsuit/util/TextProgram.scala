package com.kunyandata.nlpsuit.util

import scala.io.Source

/**
  * Created by QQ on 2016/3/18.
  *
  */

object TextProgram {

  /**
    * 调用WordSeq里面的函数实现字符串的分词和去停,并分装成方法
    * @param context 需要处理的字符串
    * @param stopWords 停用词
    * @return 返回分词去停后的结果
    */
  def process(context: String, stopWords:Array[String]): Array[String] = {

    //实现分词
    val splitWords = WordSeg.splitWord(context, 1)

    //读取分词内容并转化成Array格式
    val stopWord = WordSeg.getWords(splitWords)

    //实现去停用词
    val result = WordSeg.removeStopWords(stopWord, stopWords)
    result
  }

}
