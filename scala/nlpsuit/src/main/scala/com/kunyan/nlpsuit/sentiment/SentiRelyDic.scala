package com.kunyan.nlpsuit.sentiment

import com.kunyan.nlpsuit.util.{WordSeg, TextPreprocessing}
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by Liu on 2016/4/15.
  */
object SentiRelyDic {

  /**
    * 分词
    *
    * @param sc
    * @param sentence 输入的待分词的句子
    * @param file 用户自定义分词词典
    * @return 返回（分词结果，存储在字符串数组中）
    * @author liumiao
    */
  def cut(sc:SparkContext, sentence:String, file:String):Array[String] = {
    // RDD形式添加用户词典
    val dictionary = sc.textFile(file)
    dictionary.foreach( x => {
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
    * 坤雁分词
    *
    * @param sentence 待分析的语句
    * @param stopWordsBr 停用词典
    * @param kunyanSegTyp 分词模式
    * @return 分词结果
    * @author liumaio
    */
  def cutKunyan(sentence:String, stopWordsBr: Broadcast[Array[String]], kunyanSegTyp: Int): Array[String] ={
    // 格式化文本
    val formatedSentence = TextPreprocessing.formatText(sentence)
    // 实现分词
    val splitWords = WordSeg.splitWord(formatedSentence, kunyanSegTyp)
    // 读取分词内容并转化成Array格式
    val resultWords = WordSeg.getWords(splitWords)
    resultWords
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
  def countSenti(i:Int, sentence:Array[String], dictionary:Array[String]): Int ={
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
    * @param titleCut 当前句子的分词结果
    * @return 返回（句子的情感倾向，+1表示正向，-1表示负向，0表示中性）
    * @param file1 正向情感词词典
    * @param file2 负向情感词词典
    * @param file3 否定副词词典
    * @author liumiao
    */
  def searchSenti(sc:SparkContext, titleCut:Array[String], file1:String, file2:String, file3:String): Int ={
    // 读入词典
    val dictP = sc.textFile(file1).collect()
    val dictN = sc.textFile(file2).collect()
    val dictF = sc.textFile(file3).collect()
    // 记录正面负面倾向的次数
    var positive = 0
    var negative = 0
    // 对分词后的每一个词匹配词典
    for (i <- Range(0,titleCut.length)) {
      val tc = titleCut(i)
      // 匹配正向情感词词典
      if(dictP.contains(tc)){
        if(countSenti(i, titleCut, dictF)>0){
          positive += 1
        }
        else{
          negative += 1
        }
      }
      // 匹配负向情感词词典
      else if (dictN.contains(tc)){
        if(countSenti(i, titleCut, dictF)>0){
          negative = negative + 1
        }
        else{
          positive += 1
        }
      }
    }
    // 倾向为负面返回-1
    if ( positive < negative){
      -1
    }
    // 倾向为非负面返回1
    else{
      1
    }
  }

}
