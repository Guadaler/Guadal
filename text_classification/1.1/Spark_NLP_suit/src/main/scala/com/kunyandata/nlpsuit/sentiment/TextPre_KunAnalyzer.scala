//package com.kunyandata.nlpsuit.sentiment
//
//import java.io.{File, PrintWriter}
//
//import com.kunyandata.nlpsuit.util.{TextProcess, WordSeg}
//import org.apache.spark.SparkContext
//
///**
//  * Created by zx on 2016/3/25. 用坤雁分词器对文本进行预处理
//  */
//object TextPre_KunAnalyzer {
//
//  /**
//    * 用于对本地文本数据进行预处理（只限短文本，文本长度不可超过6Kb）
//    * 处理后单篇文章占一行，并按照“labelNum title_seg content_seg”格式写入到本地文本保存
//    *
//    * @param sc spark程序入口
//    * @param dataPath  数据本地路径，注意到父路径 如"E:\\data_test\\data"
//    * @param outPath 输出路径，完整路径 如"E:\\data_test\\textSeg.txt"
//    * @param stopWordsPath 停用词表  E:\data_test\stop_words_CN
//    * @return 无返回
//    * @author zhangxin
//    */
//  def textPreOnlyShort(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
//    val stopWords = sc.textFile(stopWordsPath).collect()
//    var writer=new PrintWriter(new File(outPath),"UTF-8")
//    //读取
//    val files =Util.readfile2HashMap(dataPath)
//    val it=files.keySet.iterator
//
//    //计数
//    var count=0
//    while(it.hasNext){
//
//      //取出单篇文章
//      val file=it.next()
//
//      //获取单篇文章title和content
//      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
//      val content=files.get(file).toString
//
//      //分词，返回分词结果为Json格式
//      val title_segJson=WordSeg.splitWord(TextProcess.formatText(title),1)
//      val content_segJson:String=WordSeg.splitWord(TextProcess.formatText(content),1)
//
//      // Json格式 ->Array数组
//      var title_seg=WordSeg.getWords(title_segJson)
//      var content_seg=WordSeg.getWords(content_segJson)
//
//      //去停，返回结果为Array
//      title_seg=TextProcess.removeStopWords(title_seg,stopWords)
//      content_seg=TextProcess.removeStopWords(content_seg,stopWords)
//
//      //结果Array =>String
//      var titlestr=""
//      var contentstr=""
//      for(word <-title_seg){
//        titlestr +=" "+word.toString()
//      }
//      for(word <-content_seg){
//        contentstr +=" "+word.toString()
//      }
//
//      //获取类别编号
//      val label=file.getParentFile.getName
//      val labelNum=
//        label match {
//          case "neg" =>1
//          case "neu" =>2
//          case "pos" =>3
//        }
//
//      //按格式写入到本地文本进行保存
//      writer.append(labelNum+" "+titlestr+" "+contentstr+"\n")
//      writer.flush()
//
//      //计数
//      count +=1
//      println("还剩下："+(files.size()-count))
//    }
//    writer.close()
//  }
//
//  /**
//    * 用于对本地文本数据进行预处理(包括长文本)
//    * 处理后单篇文章占一行，并按照“labelNum title_seg content_seg”格式写入到本地文本保存
//    * @param sc spark程序入口
//    * @param dataPath  数据本地路径，注意到父路径 如"E:\\data_test\\data"
//    * @param outPath 输出路径，完整路径 如"E:\\data_test\\textSeg.txt"
//    * @param stopWordsPath 停用词表  E:\data_test\stop_words_CN
//    * @return 无返回
//    * @author zhangxin
//    */
//  def textPre(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
//    val stopWords = sc.textFile(stopWordsPath).collect()
//    var writer=new PrintWriter(new File(outPath),"UTF-8")
//    //读取所有文章
//    val files = Util.readfile2HashMap(dataPath)
//    val it=files.keySet.iterator
//
//    //计数
//    var count=0
//    while(it.hasNext){
//      //取出单篇文章
//      val file=it.next()
//
//      //获取单篇文章title和content
//      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
//      var content=files.get(file).toString
//
//      //非法字符替换，所有title已处理过，因此无需替换
//      content =Util.replaceIllegal(content)
//
//      //计数，便于查问题
//      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)
//
//      //处理title
//      //分词，返回分词结果为Json格式
//      val title_segJson=WordSeg.splitWord(TextProcess.formatText(title),1)
//
//      // Json格式 ->Array数组
//      var title_seg=WordSeg.getWords(title_segJson)
//
//      //去停，返回结果为Array
//      title_seg=TextProcess.removeStopWords(title_seg,stopWords)
//
//      //结果Array =>String
//      var titlestr=""
//      for(word <-title_seg){
//        titlestr +=" "+word.toString()
//      }
//
//      //处理content，注意对长文本要先分段再分词
//      var contentstr=""
//      if(content.size>1500){
//        contentstr=bigText(content,stopWords)
//      }else{
//        val content_segJson=WordSeg.splitWord(TextProcess.formatText(content),1)
//        var content_seg=WordSeg.getWords(content_segJson)
//        content_seg=TextProcess.removeStopWords(content_seg,stopWords)
//        for(word <-content_seg){
//          contentstr +=" "+word.toString()
//        }
//      }
//
//      //获取文章类别编号
//      val label=file.getParentFile.getName
//      val labelNum=
//        label match {
//          case "neg" =>1
//          case "neu" =>2
//          case "pos" =>3
//        }
//
//      //按格式写入到本地文本进行保存
//      writer.append(labelNum+" "+titlestr+" "+contentstr+"\n")
//      writer.flush()
//
////      //计数
//      count +=1
////      println("还剩下："+(files.size()-count)+"  ["+label+"]  "+title)
//    }
//    writer.close()
//  }
//
//  /**
//    * 对长文章进行分段“分词+去停+格式化”，再连接成一个字符串返回
//    *
//    * @param content  待处理文章
//    * @param stopWords 停用词表
//    * @return 连接后结果字符串
//    * @author zhangxin
//    */
//  def bigText(content:String,stopWords:Array[String]): String ={
//
//    var contentstr=""
//
//    //处理整数部分，即 0 ~ 1500*（n-1）部分
//    var n=content.size/1500+1
//    for(i <- 1 until n){
//      //截取
//      var content_i=content.substring(1500*(i-1),1500*i+1)
//      //分词
//      var content_segJson =WordSeg.splitWord(TextProcess.formatText(content_i),1)
//      // Json格式 ->Array数组
//      var content_seg=WordSeg.getWords(content_segJson)
//      //去停，返回结果为Array
//      content_seg=TextProcess.removeStopWords(content_seg,stopWords)
//      //结果Array =>String
//      for(word <-content_seg){
//        contentstr +=" "+word.toString()
//      }
//    }
//
//    //处理剩下的部分
//    var content_j=content.substring(1500*(n-1),content.size)
//    var content_segJson =WordSeg.splitWord(TextProcess.formatText(content_j),1)
//    var content_seg=WordSeg.getWords(content_segJson)
//    content_seg=TextProcess.removeStopWords(content_seg,stopWords)
//    for(word <-content_seg){
//      contentstr +=" "+word.toString()
//    }
//    contentstr
//  }
//
//}
