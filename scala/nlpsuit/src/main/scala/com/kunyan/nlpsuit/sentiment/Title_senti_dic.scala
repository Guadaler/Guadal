package com.kunyan.nlpsuit.sentiment

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkContext

/**
  * Created by Liu on 2016/4/15.
  */
object Title_senti_dic {


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
    val dic = sc.textFile(file).collect()
    for(x <- dic){
      // add new words
      UserDefineLibrary.insertWord(x,"userDefine",100)
    }
    // cut sentence
    val sent = ToAnalysis.parse(sentence)
    // filter the POS tagging
    val words = for(i <- Range(0,sent.size())) yield sent.get(i).getName
    val result = new Array[String](sent.size())
    // change Vector to Array
    words.copyToArray(result)
    result
  }



  /**
    * 否定词对情感值的翻转作用
    *
    * @param i 当前情感词在句子中的位置
    * @param sentence 当前待分析的句子
    * @param dic 否定词词典
    * @return 返回（+1表示不翻转，-1表示翻转）
    * @author liumiao
    */
  def count_senti( i:Int, sentence:Array[String], dic:Array[String]): Int ={
    // 寻找情感词前面的否定词，若有则返回-1
    if (i-1 > 0){
      if (dic.contains(sentence(i-1))){
        return -1
      }
      else if (i-2 > 0){
        if (dic.contains(sentence(i-2))){
          return  -1
        }
      }
    }
    // 寻找情感词后面的否定词，若有则返回-1
    if (i+1 < sentence.length){
      if(dic.contains(sentence(i+1))){
        return -1
      }
      else if(i+2 < sentence.length){
        if (dic.contains(sentence(i+2))){
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
    * @param title_cut 当前句子的分词结果
    * @return 返回（句子的情感倾向，+1表示正向，-1表示负向，0表示中性）
    * @author liumiao
    */
  def search_senti(sc:SparkContext, title_cut:Array[String], file1:String, file2:String, file3:String): Int ={

    val dict_p = sc.textFile(file1).collect()
    val dict_n = sc.textFile(file2).collect()
    val dict_f = sc.textFile(file3).collect()

    var p = 0
    var n = 0

    // 匹配每一个词
    for (i <- Range(0,title_cut.length)) {
      val t_c = title_cut(i)
      // 匹配正向情感词词典
      if(dict_p.contains(t_c)){
        if(count_senti(i, title_cut, dict_f)>0){
          p += 1
        }
        else{
          n += 1
        }
      }
      // if word in negative dictionary
      else if (dict_n.contains(t_c)){
        if(count_senti(i, title_cut, dict_f)>0){
          n = n + 1
        }
        else{
          p += 1
        }
      }
    }

    // 倾向为负面
    if ( p < n){
      -1
    }
    // 倾向为非负面
    else{
      1
    }
  }

}
