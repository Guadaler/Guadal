package com.kunyan.tdt.process

import com.kunyan.tdt.util.LoggerUtil
import com.kunyandata.nlpsuit.util.AnsjAnalyzer

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangxin on 2016/6/28.
  * 基于事件词的主题词抽取
  * 分2层：
  * (1) 直接从事件词抽取候选词：①长词（词频） ；② 取实体词  ；③ 取重叠词
  * (2) 结合事件词，按照重叠词从备选词抽取词作为主题词
  *
  * 对外提供主题词抽取方法: getSubjectWord(communityArr:Array[String]): String
  */
object StepThree {

  /**
    * 第一层 抽候选词
    *
    * @param communityWordArr 事件词集合
    * @return 候选词集合
    */
  private def getCandiWordFromSpareWords(communityWordArr: Array[String]): Array[String] = {

    //判断事件词是否为空
    if(!communityWordArr.exists(!_.equals(""))){
      Array("kong")   //如果事件词为空集合，最后返回的主题词是kong
    }else {

      var temp = new ArrayBuffer[String]()  //临时候选词集合

      communityWordArr.map(word => {

        //① 取长词和多频词为候选
        if(spareWordModel(communityWordArr, word)){
          temp.+= (word)
        }

        //② 取实体词为候选
        if(isEntity(word.trim)){
          temp.+= (word)
        }

      })

      //③ 取重叠度最高的两个词为候选词
      val wordOnOverlap=getWordBaseOnOverlap(communityWordArr)
      temp.++=(wordOnOverlap)

      //对候选词进行过滤，并返回
      val tempSet=temp
        .map(_.replaceAll("[0-9]",""))     //过滤数字（股票代码等）
        .filter(!_.equals(""))             //过滤空白
        .toSet                             //过滤重复

      tempSet.toArray
    }

  }

  /**
    * 第二层 抽主题词
    *
    * @param CandiWords 候选词
    * @return 主题词
    */
  private def getWordsFromCandiWords(CandiWords: Array[String]): Array[String] = {


    //计算重叠度
    var overlapArr = new ArrayBuffer[Int]()  //重叠度数组

    for(n <- CandiWords.indices){

      val baseWord = CandiWords(n)
      var overlap = 0  //重叠度

      for(l <-Range(0, baseWord.length-1)){

        val win = baseWord.substring(l, l+2)

        CandiWords.filter(!_.equals(baseWord)).foreach(matchWord => {

          for(len <- Range(0, matchWord.length-1)){

            val win2 = matchWord.substring(len, len+2)

            if(win.equals(win2)) {
              overlap += 1
            }

          }

        })

      }

      overlapArr.+= (overlap)
    }

    //主题词权重 = 词长 *0.4+ 重叠度*0.6
    val wordWeight = for(n <- CandiWords.indices) yield CandiWords(n).length * 0.4 + overlapArr(n) * 0.6

    //依据权重对词排序
    val wordWeightSort: Array[String] = arrSort(CandiWords, wordWeight.toArray)

    if (wordWeightSort.length <= 2) {
      wordWeightSort
    } else {
      wordWeightSort take 2
    }

  }

  /**
    * 候选词 挑选模式
    *
    * @param words 事件词
    * @param word 待挑选词
    * @return 是否为挑选为候选词
    */
  private def spareWordModel(words: Array[String], word: String): Boolean = {

    //通过词长
    if(word.trim.length >= 4) return true

    //其他词的包含词 +词长>3
    if(!words.map(_.contains(word)).isEmpty && word.length >= 3) return true

    //是其他词包含词 +词频>3
    if(words.map(_.contains(word)).length >= 3 ) return true

    false
  }

  /**
    * 验证是否实体词
    * Ansj标注词性，筛选出实体词
    *
    * @param word 待挑选词
    * @return 是否为挑选为候选词
    */
  private def isEntity(word: String): Boolean ={

    val wordSeg = AnsjAnalyzer.cutWithTag(word + "，")
    val pos = wordSeg(1).getNatrue.natureStr

    pos match {

      case "nr" => true  //人名
      case "nw" => true  //新词
      case "ns" => true  //地名
      case "nt" => true  //机构团体
      case "nz" => true  //其他专名
      case _ => false

    }

  }


  /**
    * 获取重叠度最高的两个词作为候选词
    *
    * @param communityWordArr 事件词
    * @return 候选词
    */
  private def getWordBaseOnOverlap(communityWordArr: Array[String]): Array[String] ={

    var wordTemp_1 = ""  //重叠度高词1
    var wordTemp_2 = ""  //重叠度高词2
    var count: Int = 0  //重叠度
    val resultArr = new ArrayBuffer[String]()

    for(n <- communityWordArr.indices){

      val baseWord = communityWordArr(n)
      val countTemp = 0  //重叠度

      for(l <- Range(0,baseWord.length - 1)){

        val win=baseWord.toString.substring(l, l+2)  //截取baseWord的窗口部分

        communityWordArr.filter(!_.equals(baseWord)).foreach(matchWord => {

          for(len <-Range(0, matchWord.length-1)){

            val win2 = matchWord.substring(len, len+2)    //截取matchWord的窗口部分
            if(win.equals(win2)) count += 1

          }

        })

      }

      //count 默认为0，重叠度为0不加入候选词
      if(countTemp > 0 && (countTemp >= count || countTemp > count-1)) {

        count = countTemp
        wordTemp_2 = wordTemp_1
        wordTemp_1 = baseWord.toString

      }
    }

    if(!wordTemp_1.equals("")){
      resultArr.+= (wordTemp_1)
      resultArr.+= (wordTemp_2)
    }

    wordTemp_1 = ""
    wordTemp_2 = ""
    count = 0

    resultArr.toArray
  }


  /**
    * 数组按对应的权值排序
    * stringArr和wightArr要一一对应
    *
    * @param stringArr 待排序数组
    * @param wightArr 对应的权值
    * @return 返回排序结果
    */
  private def arrSort(stringArr: Array[String], wightArr: Array[Double]): Array[String] = {

    var tempStr = ""      //临时存放关键词
    var tempWeight = 0.0  //临时存放权值

    val sBuff = stringArr.toBuffer  //转成变长数组
    val wBuff = wightArr.toBuffer

    for(n <- wightArr.indices){

      var weight_1 = wBuff(n)

      for(m <- Range(n, wightArr.length)){

        var weight_2 = wBuff(m)

        if(weight_1 < weight_2) {

          //调整weight数组顺序
          tempWeight = weight_1
          weight_1 = weight_2
          weight_2 = tempWeight
          wBuff(n) = weight_2
          wBuff(m) = tempWeight

          //对应调整string数组顺序
          tempStr = sBuff(n)
          sBuff(n) = sBuff(m)
          sBuff(m) = tempStr

        }

      }

    }

    sBuff.toArray
  }


  /**
    * 事件主题词提取
    *
    * @param communityArr 事件词
    * @return 一个主题词
    * @author zhangxin
    */
  def getSubjectWord(communityArr:Array[String]): String = {

    //候选词抽取
    val candiWords = getCandiWordFromSpareWords(communityArr)
    LoggerUtil.warn("完成候选词提取！ 》》》》》》》》》》》》》")

    //主题词抽取
    val words = getWordsFromCandiWords(candiWords)
    LoggerUtil.warn("完成主题词提取！ 》》》》》》》》》》》》》")

    //结果返回
    words(0)
  }

}

