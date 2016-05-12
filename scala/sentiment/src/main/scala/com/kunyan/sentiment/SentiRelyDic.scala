package com.kunyan.sentiment

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis

/**
  * Created by liumiao on 2016/4/21.
  * 基于词典的情感分析类
  */
object SentiRelyDic {

  /**
    * 添加用户词典
    * @param userDict 用户自定义分词词典
    * @author liumiao
   */
  def addUserDic(userDict: Array[String]): Unit = {

    userDict.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 100)
    })

  }

  /**
    * 分词
    * @param sentence 输入的待分词的句子
    * @return 分词结果，存储在字符串数组中
    * @author liumiao
    */
  private def cut(sentence: String): Array[String] = {

    // 利用ansj分词并过滤词性标注
    val sentenceCut = ToAnalysis.parse(sentence)
    val words = for (i <- Range(0,sentenceCut.size())) yield sentenceCut.get(i).getName

    words.toArray
  }

  /**
    * 寻找否定词
    * @param i 当前情感词在句子中的位置
    * @param sentence 当前待分析的句子
    * @param dictionary 否定词词典
    * @return +1表示不翻转，-1表示翻转
    * @author liumiao
    */
  private def searchNeg(i: Int, sentence: Array[String], dictionary: Array[String]): Int = {

    // 寻找情感词前面的否定词，若有则返回-1
    if (i-1 > 0) {

      if (dictionary.contains(sentence(i-1))) {
        return -1
      } else if (i-2 > 0) {

        if (dictionary.contains(sentence(i-2)))
          return -1

      }

    }

    // 寻找情感词后面的否定词，若有则返回-1
    if (i+1 < sentence.length) {

      if (dictionary.contains(sentence(i+1))) {
        return -1
      } else if (i+2 < sentence.length) {

        if (dictionary.contains(sentence(i+2)))
          return -1

      }

    }

    // 匹配不到否定词，则返回1
    1
  }

  /**
    * 情感分析
    * @param title 当前句子的分词结果
    * @param dicMap 词典
    * @return 句子的情感倾向，+1表示正向，-1表示负向，0表示中性
    * @author liumiao
    */
  def predictSenti(title: String, dicMap: Map[String, Array[String]],
                   kunyanConf: KunyanConf): String = {

    // 记录正面负面倾向的次数
    var positive = 0
    var negative = 0

    // 对标题分词
    val titleCut = TextPreprocessing.process(title, dicMap("stopWordsCN"), kunyanConf)

    // 对分词后的每一个词匹配词典
    for (i <- titleCut.indices) {

      val tc = titleCut(i)

      // 匹配正向情感词词典，若匹配不到，则匹配负向情感词词典
      if (dicMap("dictP").contains(tc)) {

        if (searchNeg(i, titleCut, dicMap("dictF"))>0) {
          positive += 1
        } else {
          negative += 1
        }

      } else if (dicMap("dictN").contains(tc)) {

        if (searchNeg(i, titleCut, dicMap("dictF"))>0) {
          negative += 1
        } else {
          positive += 1
        }

      }

    }

    // 倾向为负面返回"neg"，否则返回"pos"
    if ( positive < negative) {
      "neg"
    } else {
      "pos"
    }

  }

}
