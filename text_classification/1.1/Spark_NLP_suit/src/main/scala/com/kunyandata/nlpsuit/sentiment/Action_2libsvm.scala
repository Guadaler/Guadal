package com.kunyandata.nlpsuit.sentiment

import java.io.{File, PrintWriter}
import java.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by zx on 2016/3/16.
  */
object Action_2libsvm {
  def main(args: Array[String]) {
      var filepath="src/main/resource/sourcedata/data"

      var file_map=IO.readfile2(filepath);
      var word_map=new util.HashMap[File,util.HashMap[String,Int]] //所有文章词对  文章File：[词：Tf-IDF值]
      var wordsDict=new util.HashMap[String,Int]  //词典 编号:词
      var label_map=new util.HashMap[String,Int]  //类别 neg:1  pos:2  neu:3

      //初始化label
      label_map=Util.loadlabel_map()

      //初始化分词组件
      var config=new SparkConf().setAppName("News_title_sentiment_lm").setMaster("local")
      var sc=new SparkContext(config)
      Analyzer.add_dic("E:\\dict\\senti_dict\\user_dict.txt",sc)

      //file_map => word_map
      var it1=file_map.keySet().iterator()
      while(it1.hasNext){
          var file=it1.next()
          var content=file_map.get(file)
          var content2=content.replace(" ","")  //去空格
          var content3=content2.trim()  //去空格
          var result=Analyzer.Nlp_cut(content3)

          var onefile_content=Analyzer.removeUsenelss(result)
          Analyzer.add2wordsDict(wordsDict,onefile_content)
          word_map.put(file,onefile_content)
      }

      //将“词典”写入到文件
      var dictPath="src/main/resource/sourcedata/wordsDict.txt"
      var it3=wordsDict.keySet().iterator()
      var writer=new PrintWriter(dictPath)
      while(it3.hasNext){
          var word=it3.next()
//          println(wordsDict.get(word)+" : "+word)
          writer.append(wordsDict.get(word)+" "+word+"\n")
          writer.flush()
      }
      writer.close()

      //word_map => word_map_tfidf
      //         => libsvm
      //         => outPath
      var libsvmPath="src/main/resource/sourcedata/libsvm.txt"
      var writer2=new PrintWriter(libsvmPath)

      var it2=word_map.keySet().iterator()
      while(it2.hasNext){
          var onefile_url=it2.next()
          var label=Util.getLabel(onefile_url)
          var labelNum=label_map.get(label)
          writer2.append(labelNum+" ")

          val keys =ListBuffer[Int]()
          var keys_value=new util.HashMap[Integer, Double];

          var onefile=word_map.get(onefile_url)
          var it3=onefile.keySet().iterator()
          while(it3.hasNext){
              var word=it3.next()
              var wordNum=wordsDict.get(word);
              var tf_idf=Tf_Idf.getTf_Idf(word,onefile,word_map);

              keys.append(wordNum)
              keys_value.put(wordNum,tf_idf);
//              println(word+"  :"+tf_idf)
          }

          var key=keys.sorted;
          for(i <- key){
              writer2.append(i.toString+":"+keys_value.get(i)+" ")
//              println(i.toString+":"+keys_value.get(i)+" ")
          }
          writer2.println();
          writer2.flush();
          println()
      }
      writer2.flush();
      writer2.close();
  }


}
