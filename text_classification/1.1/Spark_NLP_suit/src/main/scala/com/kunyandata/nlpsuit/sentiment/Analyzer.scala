package com.kunyandata.nlpsuit.sentiment

import java.util

import org.ansj.domain.Term
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by zx on 2016/3/8
  * 基于ansj的切词工具测试
  */

object Analyzer extends App{

  override def main(args: Array[String]) {
    var conf=new SparkConf().setAppName("News_title_sentiment_lm").setMaster("local")
    val sc = new SparkContext(conf)

    val str = "金融行业的寒冬即将到来了，大家做好心理准备，云海金属是一家不错的公司，金融市场依然很严峻，血虚证的概念比较难"

    //标准分词
    var result=cut(str)
    for(value:String <- result){
      print(value+"  ")
    }

    //添加用户词典
    println()
    println("【测试用户添加词典效果】")
    var user_dic="E:\\dict\\senti_dict\\user_dict.txt"
    add_dic(user_dic,sc)
    var result2=cut(str)
    for(value:String <- result2){
      print(value+"  ")
    }

    //NLP分词
    println()
    var result3=Nlp_cut(str)
    for(value:Term <-result3){
        print(value.toString+"  ")
    }

    //测试剔除无用词
    println()
    removeUsenelss(result3)

    //获取剔除了停用词+词频的结果
    var wordsMap=removeUsenelss(result3)
    var wordSet=wordsMap.keySet()
    for(key:String <- wordSet){
        println(key+"  "+wordsMap.get(key))
    }
  }

  /**
    * ansj分词器初始化
    * @param sc  spark程序入口
    * @param user_dic  用户词典数组
    * @return 无返回
    * @author zhangxin
    */
  def init(sc:SparkContext,user_dic:Array[String]): Unit ={
    //添加用户词典
    for(dic <- user_dic){
      add_dic(dic,sc)
    }


  }
  /**
    * 添加用户词典到分词器
    *
    * @param path
    * @param sc
    * @return
    */
  def add_dic(path:String,sc:SparkContext):Unit ={
      //println("路径："+path)
      val dic=sc.textFile(path).collect()
      for(x:String <- dic){
        UserDefineLibrary.insertWord(x,"userDefine",100)
      }
  }

  /**
    * 标准分词 （不带词性标注）
    */
  def cut(sentence:String):Array[String]={
      // cut sentence
      val sent = ToAnalysis.parse(sentence)  //核心切词操作

      // filter the POS tagging
      val words = for(i <- Range(0,sent.size())) yield sent.get(i).getName
      val result = new Array[String](sent.size())

      // change Vector to Array
      words.copyToArray(result)
      result
  }

  /**
    * 自然语言分词,结果不稳定但是很全面 (带词性标注)
    *
    * @param sentence  待分词句子
    * @return  分词结果
    */
  def Nlp_cut(sentence:String):Array[Term]={
      // cut sentence
      val sent = NlpAnalysis.parse(sentence)

      // List => Vector
      val words= for(i <- Range(0,sent.size())) yield sent.get(i).getFrom

      val result = new Array[Term](sent.size())
    // change Vector to Array
      words.copyToArray(result)
      result
  }

  /**
    * 根据词性剔除无用词
    *
    * @param words
    */
  def removeUsenelss(words:Array[Term]):util.HashMap[String,Int]={
      var words_fre_map=new util.HashMap[String,Int]()
      if(words!=null){
        for(word:Term<-words){
          var wordstr=word.toString();
          if(!wordstr.contains("##") && wordstr.contains("/")){
            var item=wordstr.substring(0,wordstr.indexOf("/"))
            var ext=wordstr.substring(wordstr.indexOf("/")+1,wordstr.length())
            if(!ext.startsWith("uj")&& !ext.startsWith("ul")&& !ext.startsWith("w") && !ext.startsWith("m")){
              addMap(words_fre_map,item.trim)   //将符合条件的item添加到words_fre_map中
            }
          }
        }
      }
    words_fre_map
  }

  /**
    * 词频统计
    *
    * @param map  存放结果
    * @param item  关键词
    */
  def addMap(map:util.HashMap[String,Int],item:String):Unit={
      if(!map.containsKey(item)){
          map.put(item,1)
      }else{
          map.put(item,map.get(item)+1)
      }
  }

  /**
    * 将词加入到“词典”[wordsDict]
    *
    * @param wordsDict  词典 编号:词
    * @param words_fre_map  一篇文章  词：tf-idf值
    */
  def add2wordsDict(wordsDict:util.HashMap[String,Int],words_fre_map:util.HashMap[String,Int]): Unit ={
      var count=wordsDict.size();
      var it=words_fre_map.keySet().iterator();
      while(it.hasNext){
         count +=1
         var word=it.next();
         if(!wordsDict.keySet().contains(word)){
            wordsDict.put(word,count)
//            println(count+":"+word)
         }
      }
  }
}
