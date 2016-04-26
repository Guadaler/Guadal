package com.kunyan.sentiment

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/4/21.
  */
object SentiRelyDic {

  /**
    * 分词
    *
    * @param sentence 输入的待分词的句子
    * @param userDic 用户自定义分词词典
    * @return 返回（分词结果，存储在字符串数组中）
    * @author liumiao
    */
  private def cut(sentence:String, userDic:Array[String]):Array[String] = {
    // RDD形式添加用户词典
    userDic.foreach( x => {
      UserDefineLibrary.insertWord(x,"userDefine",100)
    })
    // 分词
    val sentenceCut = ToAnalysis.parse(sentence)
    // 过滤词性标注
    val words = for(i <- Range(0,sentenceCut.size())) yield sentenceCut.get(i).getName
    val result = new Array[String](sentenceCut.size())
    // 将 Vector 转换为 Array
    words.copyToArray(result)
    result
  }

  /**
    * 否定词对情感值的翻转作用
    *
    * @param i 当前情感词在句子中的位置
    * @param sentence 当前待分析的句子
    * @param dictionary 否定词词典
    * @return 返回（+1表示不翻转，-1表示翻转）
    * @author liumiao
    */
  private def countSenti(i:Int, sentence:Array[String], dictionary:Array[String]): Int ={
    // 寻找情感词前面的否定词，若有则返回-1
    if (i-1 > 0){
      if (dictionary.contains(sentence(i-1))){
        return -1
      }
      else if (i-2 > 0){
        if (dictionary.contains(sentence(i-2))){
          return  -1
        }
      }
    }
    // 寻找情感词后面的否定词，若有则返回-1
    if (i+1 < sentence.length){
      if(dictionary.contains(sentence(i+1))){
        return -1
      }
      else if(i+2 < sentence.length){
        if (dictionary.contains(sentence(i+2))){
          return -1
        }
      }
    }
    // 匹配不到否定词，则返回1
    1
  }

  /**
    * 情感分析
    *
    * @param title 当前句子的分词结果
    * @return 返回（句子的情感倾向，+1表示正向，-1表示负向，0表示中性）
    * @param dicBuffer 词典
    * @author liumiao
    */
  def searchSenti(title:String, dicBuffer:ArrayBuffer[Array[String]]): String={
    // 记录正面负面倾向的次数
    var positive = 0
    var negative = 0
    // 对标题分词
    val titleCut = SentiRelyDic.cut(title, dicBuffer(0))
    // 对分词后的每一个词匹配词典
    for (i <- titleCut.indices) {
      val tc = titleCut(i)
      // 匹配正向情感词词典
      if(dicBuffer(1).contains(tc)){
        if(countSenti(i, titleCut, dicBuffer(3))>0){
          positive += 1
        }
        else{
          negative += 1
        }
      }
      // 匹配负向情感词词典
      else if (dicBuffer(2).contains(tc)){
        if(countSenti(i, titleCut, dicBuffer(3))>0){
          negative = negative + 1
        }
        else{
          positive += 1
        }
      }
    }
    // 倾向为负面返回"neg"
    if ( positive < negative){
      "neg"
    }
    // 倾向为非负面返回"pos"
    else{
      "pos"
    }
  }

}
