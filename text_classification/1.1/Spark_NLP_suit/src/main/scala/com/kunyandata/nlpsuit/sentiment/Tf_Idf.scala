package com.kunyandata.nlpsuit.sentiment

import java.io.File
import java.util

/**
  * Created by zx on 2016/3/16.
  */
object Tf_Idf {

  def main(args: Array[String]) {

  }

  /**
    * 计算TF-Idf
    */
  def getTf_Idf(item:String,
                article:util.HashMap[String, Int],
                word_map:util.HashMap[File, util.HashMap[String, Int]]): Double ={
      var tf=getTf(item:String, article:util.HashMap[String, Int])
      var idf=getIdf(item:String,word_map:util.HashMap[File, util.HashMap[String, Int]])
      var Tf_Idf=tf*idf;
      Tf_Idf
  }
  /**
    * 计算tf
    */
  def getTf(item:String, article:util.HashMap[String, Int]):Double ={
      var tf:Double=0.000000
      var count=article.get(item);  //该词的词频
      var sum=0;  //该文章所有词数
      var it=article.values().iterator()
      while (it.hasNext){
          var key=it.next();
          sum +=key
      }
      if(sum !=0){
        tf=article.get(item).toDouble/sum.toDouble
      }
      tf
  }

  /**
    * 计算IDF
    * @param item
    * @param word_map
    * @return
    */
  def getIdf(item:String,word_map:util.HashMap[File, util.HashMap[String, Int]]): Double ={
      var idf:Double=0.000000;
      var count=0;
      var it=word_map.keySet().iterator()
      while (it.hasNext){
        var key=it.next();
        var onefile=word_map.get(key)
        if(onefile.keySet().contains(item)){
          count +=1;
        }
      }
//      idf=word_map.size().toDouble/count.toDouble/
      idf=Math.log(word_map.size().toDouble/count.toDouble)
      println("idf:"+idf+"="+count.toDouble+"  "+word_map.size().toDouble)
      idf
  }

}
