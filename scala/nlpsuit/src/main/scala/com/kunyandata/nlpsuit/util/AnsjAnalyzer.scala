package com.kunyandata.nlpsuit.util

import java.util
import org.ansj.domain.Term
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by zx on 2016/3/8   基于ansj的分词工具
  */
object AnsjAnalyzer extends App{

  override def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("News_title_sentiment_lm").setMaster("local")
    val sc = new SparkContext(conf)
    val str = "金融行业的寒冬即将到来了，大家做好心理准备，云海金属是一家不错的公司，金融市场依然很严峻，血虚证的概念比较难"

    //用户词典数组
    val userDict=Array("E:\\dict\\senti_dict\\user_dict.txt")

    //分词器初始化
    init(sc,userDict)

    //标准分词
    var result=cut(str)
    for(value:String <- result){
      print(value+"  ")
    }

    //NLP分词
    println()
    var result3=Nlp_cut(str)
    for(value:Term <-result3){
      print(value.toString+"  ")
    }

    //剔除无用词
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
    * @return 无
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
    * @param path  词典路径
    * @param sc spark程序入口
    * @return 无
    * @author zhangxin
    */
  def add_dic(path:String,sc:SparkContext):Unit ={
    val dic=sc.textFile(path).collect()
    for(x:String <- dic){
      UserDefineLibrary.insertWord(x,"userDefine",100)
    }
  }

  /**
    * 标准分词 ，无词性标注
    * @param sentence  待分词语句
    * @return 分词结果（数组）
    * @author zhangxin
    */
  def cut(sentence:String):Array[String]={
    //切词
    val sent = ToAnalysis.parse(sentence)
    //过滤词性标注
    val words = for(i <- Range(0,sent.size())) yield sent.get(i).getName
    val result = new Array[String](sent.size())
    // change Vector to Array
    words.copyToArray(result)

    result
  }

  /**
    * 自然语言分词，带词性标注
    * @param sentence  待分词句子
    * @return  分词结果（数组）
    * @author zhangxin
    */
  def Nlp_cut(sentence:String):Array[Term]={
    // 切词
    val sent = NlpAnalysis.parse(sentence)
    // List => Vector
    val words= for(i <- Range(0,sent.size())) yield sent.get(i).getFrom
    //定义数组
    val result = new Array[Term](sent.size())
    // change Vector to Array
    words.copyToArray(result)
    result
  }

  /**
    * 根据词性剔除无用词
    * @param words 分词结果数组
    * @return 处理结果Map[String,Int]  [关键词，词频]
    * @author zhangxin
    */
  def removeUsenelss(words:Array[Term]):util.HashMap[String,Int]={
    var words_fre_map=new util.HashMap[String,Int]()
    if(words!=null){
      for(word:Term<-words){
        val wordstr=word.toString();
        if(!wordstr.contains("##") && wordstr.contains("/")){
          val item=wordstr.substring(0,wordstr.indexOf("/"))
          val ext=wordstr.substring(wordstr.indexOf("/")+1,wordstr.length())
          if(!ext.startsWith("uj")&& !ext.startsWith("ul")&& !ext.startsWith("w") && !ext.startsWith("m")){
            //将符合条件的item添加到words_fre_map中
            addMap(words_fre_map,item.trim)
          }
        }
      }
    }
    words_fre_map
  }

  /**
    * 词频统计
    * @param map  存放结果
    * @param item  关键词
    * @author zhangxin
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
    * @param wordsDict  词典格式：  编号:关键词
    * @param words_fre_map  一篇文章  关键词：tf-idf值
    */
  def add2wordsDict(wordsDict:util.HashMap[String,Int],words_fre_map:util.HashMap[String,Int]): Unit ={
    var count=wordsDict.size();
    val it=words_fre_map.keySet().iterator();
    while(it.hasNext){
      count +=1
      var word=it.next();
      if(!wordsDict.keySet().contains(word)){
        wordsDict.put(word,count)
      }
    }
  }
}
