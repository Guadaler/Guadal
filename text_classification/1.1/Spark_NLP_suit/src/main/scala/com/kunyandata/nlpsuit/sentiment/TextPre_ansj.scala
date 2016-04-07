package com.kunyandata.nlpsuit.sentiment

import java.io.{File, PrintWriter}

import com.kunyandata.nlpsuit.util.TextProcess
import org.apache.spark.SparkContext

/**
  * Created by zx on 2016/3/25. 用ansj分词器对文本进行预处理
  */
object TextPre_ansj {

  /**
    * 用于对本地文本数据进行预处理
    * 处理后单篇文章占一行，并按照“labelNum title_seg content_seg”格式写入到本地文本保存
    * @param sc spark程序入口
    * @param dataPath  数据本地路径，注意到父路径 如"E:\\data_test\\data"
    * @param outPath 输出路径，完整路径 如"E:\\data_test\\textSeg.txt"
    * @param stopWordsPath 停用词表  E:\data_test\stop_words_CN
    * @return 无返回
    * @author zhangxin
    */
  def textPre(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer=new PrintWriter(new File(outPath),"UTF-8")
    //读取文件
    val files=Util.readfile2HashMap(dataPath)
    val it=files.keySet.iterator

    //分词器初始化，加载用户词典
    val user_dict=Array(
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\user_dict.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\neg_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\nega_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\posi_dic.txt",
      "E:\\dict\\senti_dict\\user_dict.txt"
    )
    Analyzer.init(sc,user_dict)

    //计数
    var count=0
    while(it.hasNext){
      //取出单篇文章
      val file=it.next()

      //获取单篇文章title和content
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=files.get(file).toString

      //计数，便于查问题
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      //分词，返回分词结果为Arry
      var title_seg=Analyzer.cut(title)
      var content_seg=Analyzer.cut(content)

      //去停，返回结果为Array
      title_seg=TextProcess.removeStopWords(title_seg,stopWords)
      content_seg=TextProcess.removeStopWords(content_seg,stopWords)

      //结果Array =>String
      var titlestr=""
      var contentstr=""
      for(word <-title_seg){
        titlestr +=","+word.toString()
      }
      for(word <-content_seg){
        contentstr +=" "+word.toString()
      }

      //获取类别编号
      val label=file.getParentFile.getName
      val labelNum=
        label match {
          case "neg" =>1
          case "neu" =>2
          case "pos" =>3
        }

      //按格式写入到本地文本进行保存
      writer.append(labelNum+"#"+titlestr+" "+contentstr+"\n")
      writer.flush()

      //计数
      count +=1
    }
    writer.close()
  }

  /**
    * 只对本地文本【标题】进行预处理
    * @param sc spark程序入口
    * @param dataPath  数据本地路径，注意到父路径 如"E:\\data_test\\data"
    * @param outPath 输出路径，完整路径 如"E:\\data_test\\textSeg.txt"
    * @param stopWordsPath 停用词表  E:\data_test\stop_words_CN
    * @return 无返回
    * @author zhangxin
    */
  def textPre_title(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer=new PrintWriter(new File(outPath),"UTF-8")
    //读取文件
    val files=Util.readfile2HashMap(dataPath)
    val it=files.keySet.iterator

    //分词器初始化，加载用户词典
    val user_dict=Array(
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\user_dict.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\neg_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\nega_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\posi_dic.txt",
      "E:\\dict\\senti_dict\\user_dict.txt"
    )
    Analyzer.init(sc,user_dict)

    //计数
    var count=0
    while(it.hasNext){
      //取出单篇文章
      val file=it.next()

      //获取单篇文章title
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))

      //计数，便于查问题
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      //分词，返回分词结果为Arry
      var title_seg=Analyzer.cut(title)

      //去停，返回结果为Array
      title_seg=TextProcess.removeStopWords(title_seg,stopWords)

      //结果Array =>String
      var titlestr=""
      for(word <-title_seg){
        titlestr +=","+word.toString()
      }

      //获取类别编号
      val label=file.getParentFile.getName
      val labelNum=
        label match {
          case "neg" =>1
          case "neu" =>2
          case "pos" =>3
        }

      //按格式写入到本地文本进行保存
      writer.append(labelNum+"#"+titlestr+"\n")
      writer.flush()

      //计数
      count +=1
    }
    writer.close()
  }

  /**
    * 只对本地文本【内容】数据进行预处理
    * @param sc spark程序入口
    * @param dataPath  数据本地路径，注意到父路径 如"E:\\data_test\\data"
    * @param outPath 输出路径，完整路径 如"E:\\data_test\\textSeg.txt"
    * @param stopWordsPath 停用词表  E:\data_test\stop_words_CN
    * @return 无返回
    * @author zhangxin
    */
  def textPre_content(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer=new PrintWriter(new File(outPath),"UTF-8")
    //读取文件
    val files=Util.readfile2HashMap(dataPath)
    val it=files.keySet.iterator

    //分词器初始化，加载用户词典
    val user_dict=Array(
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\user_dict.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\neg_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\nega_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\posi_dic.txt",
      "E:\\dict\\senti_dict\\user_dict.txt"
    )
    Analyzer.init(sc,user_dict)

    //计数
    var count=0
    while(it.hasNext){
      //取出单篇文章
      val file=it.next()

      //获取单篇文章title和content
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=files.get(file).toString

      //计数，便于查问题，title便于知道是哪篇文章
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      //分词，返回分词结果为Arry
      var content_seg=Analyzer.cut(content)

      //去停，返回结果为Array
      content_seg=TextProcess.removeStopWords(content_seg,stopWords)

      //结果Array =>String
      var contentstr=""
      for(word <-content_seg){
        contentstr +=","+word.toString()
      }

      //获取类别编号
      val label=file.getParentFile.getName
      val labelNum=
        label match {
          case "neg" =>1
          case "neu" =>2
          case "pos" =>3
        }

      //按格式写入到本地文本进行保存
      writer.append(labelNum+"#"+contentstr+"\n")
      writer.flush()

      //计数
      count +=1
    }
    writer.close()
  }


  def textPre_content_F(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer=new PrintWriter(new File(outPath),"UTF-8")
    //读取文件
    val files=Util.readfile2HashMap(dataPath)
    val it=files.keySet.iterator

    //分词器初始化，加载用户词典
    val user_dict=Array(
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\user_dict.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\neg_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\nega_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\posi_dic.txt",
      "E:\\dict\\senti_dict\\user_dict.txt"
    )
    Analyzer.init(sc,user_dict)

    //计数
    var count=0
    while(it.hasNext){

      //取出单篇文章
      val file=it.next()

      //获取单篇文章title和content
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=files.get(file).toString

      //计数，便于查问题，title便于知道是哪篇文章
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      //分词，返回分词结果为Arry
      var content_seg=Analyzer.cut(content)

      //去停，返回结果为Array
      content_seg=TextProcess.removeStopWords(content_seg,stopWords)

      //结果Array =>String
      var contentstr=""
      for(word <-content_seg){
        contentstr +=","+word.toString()
      }

      //获取类别编号
      val label=file.getParentFile.getName
      val labelNum=
        label match {
          case "neg" =>1
          case "neu" =>4   //中性标注为 4
          case "pos" =>4   //中性标注为 4
        }

      //按格式写入到本地文本进行保存
      writer.append(labelNum+"#"+contentstr+"\n")
      writer.flush()

      //计数
      count +=1
    }
    writer.close()
  }

  def textPre_content_S(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer=new PrintWriter(new File(outPath),"UTF-8")
    //读取文件
    val files=Util.readfile2HashMap(dataPath)
    val it=files.keySet.iterator

    //分词器初始化，加载用户词典
    val user_dict=Array(
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\user_dict.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\neg_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\nega_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\posi_dic.txt",
      "E:\\dict\\senti_dict\\user_dict.txt"
    )
    Analyzer.init(sc,user_dict)

    //计数
    var count=0
    while(it.hasNext){

      //取出单篇文章
      val file=it.next()

      //获取单篇文章title和content
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=files.get(file).toString

      //计数，便于查问题，title便于知道是哪篇文章
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      //分词，返回分词结果为Arry
      var content_seg=Analyzer.cut(content)

      //去停，返回结果为Array
      content_seg=TextProcess.removeStopWords(content_seg,stopWords)

      //结果Array =>String
      var contentstr=""
      for(word <-content_seg){
        contentstr +=","+word.toString()
      }

      //获取类别编号
      val label=file.getParentFile.getName
      val labelNum=
        label match {
          case "neg" =>1
          case "neu" =>2
          case "pos" =>3
        }

      //按格式写入到本地文本进行保存
      if(labelNum==2){
        writer.append(labelNum+"#"+contentstr+"\n")
        writer.flush()
      }else if(labelNum==3){
        writer.append(labelNum+"#"+contentstr+"\n")
        writer.flush()
      }

      //计数
      count +=1
    }
    writer.close()
  }

  def textPre_content_FS(sc:SparkContext,dataPath:String,outPath_F:String,outPath_S:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer1=new PrintWriter(new File(outPath_F),"UTF-8")
    val writer2=new PrintWriter(new File(outPath_S),"UTF-8")

    //读取文件
    val files=Util.readfile2HashMap(dataPath)
    val it=files.keySet.iterator

    //分词器初始化，加载用户词典
    val user_dict=Array(
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\user_dict.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\neg_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\nega_dic.txt",
      "text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\posi_dic.txt",
      "E:\\dict\\senti_dict\\user_dict.txt"
    )
    Analyzer.init(sc,user_dict)

    //计数
    var count=0
    while(it.hasNext){

      //取出单篇文章
      val file=it.next()

      //获取单篇文章title和content
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=files.get(file).toString

      //计数，便于查问题，title便于知道是哪篇文章
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      //分词，返回分词结果为Arry
      var content_seg=Analyzer.cut(content)

      //去停，返回结果为Array
      content_seg=TextProcess.removeStopWords(content_seg,stopWords)

      //结果Array =>String
      var contentstr=" "
      for(word <-content_seg){
        contentstr +=","+word.toString().trim
      }

      //获取类别编号
      val label=file.getParentFile.getName

      //获取类别编号
      val labelNum_f=
        label match {
          case "neg" =>1
          case "neu" =>4   //中性标注为 4
          case "pos" =>4   //中性标注为 4
        }
      //按格式写入到本地文本进行保存
      writer1.append(labelNum_f+"#"+contentstr+"\n")
      writer1.flush()

      val labelNum_s=
        label match {
          case "neg" =>1
          case "neu" =>2
          case "pos" =>3
        }
      //按格式写入到本地文本进行保存
      if(labelNum_s==2){
        writer2.append(labelNum_s+"#"+contentstr+"\n")
        writer2.flush()
      }else if(labelNum_s==3){
        writer2.append(labelNum_s+"#"+contentstr+"\n")
        writer2.flush()
      }

      //计数
      count +=1
    }
    writer1.close()
    writer2.close()
  }
}
