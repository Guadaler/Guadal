package tdt

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import sentiment.Util
import scala.collection.mutable.{HashMap, Set}
import scala.io.Source

/**
  * Created by zx on 2016/5/5.
  * LDA 文本格式预处理
  */
object TextPre {
  def main(args: Array[String]) {


    val inFile="D:\\222_TDT\\text05.txt"
    val outFile="D:\\222_TDT\\text05_pre.txt"
    val wordsDic="D:\\222_TDT\\text05_dic.txt"
    val conf=new SparkConf().setAppName("LDA").setMaster("local")
    val sc=new SparkContext(conf)

    //inHankcs -> outHankcs ->outPreHankcs
    val inHankcs="D:\\222_TDT\\data_hankcs\\mini"
    val outHankcs="D:\\222_TDT\\data_hankcs\\datahankcs.txt"
    val outHankcs_zx="D:\\222_TDT\\data_hankcs\\datahankcs_zx.txt"
    val outPreHankcs="D:\\222_TDT\\data_hankcs\\datahankcs_pre_feature.txt"
    val wordsDicHankcs="D:\\222_TDT\\data_hankcs\\datahankcs_dic_feature.txt"

//    textHankcs2Txt(inHankcs,outHankcs)


//    allWordsDic(outHankcs_zx,wordsDicHankcs,sc)
//    allWordsDic(outHankcs_zx,wordsDicHankcs,sc,true)

    textPre2VSM(outHankcs_zx,outPreHankcs,wordsDicHankcs,sc)

  }

  //**********************************************************************************************
  //【预处理的一些方法】
  //**********************************************************************************************

  /**
    * 从所有的文章获取单词组成词典（不重复）
    *
    * @param inFile  txt源文件文件，分词去停，英文逗号隔
    * @return
    */
  def allWordsDic(inFile:String,wordsDic:String,sc:SparkContext): Set[String] ={
    val wr=new PrintWriter(wordsDic,"utf-8")
    //词典表
    val words=Set[String]()

    val data=sc.textFile(inFile).map(line=> {
      val temp = line.split("#")
      val contentArr = if (temp.length > 1) {
        temp(1).trim.split(" ")
      } else {
        Array("开始")
      }
      contentArr
    }).collect()

    //将生词加入到words
    data.foreach(line=>{
      line.foreach(word=>{
        var temp=word.replaceAll("\\s*","")
        temp=temp.replaceAll(" ","")
        temp=temp.replaceAll(" ","")
        temp=temp.replaceAll("\t","")
        temp=temp.replaceAll(" +","")
        words +=temp
      })
    })
    //遍历并写出
    words.foreach(word=>wr.append(word.trim+"\n").flush())
    words
  }

  /**
    * 加入特征提取，达到降维目的
    * @param inFile
    * @param wordsDic
    * @param sc
    * @param isFeature 是否进行特征提取的flag
    * @return Set集合，无重复项
    */
  def allWordsDic(inFile:String,wordsDic:String,sc:SparkContext,isFeature:Boolean): Set[String] ={
    val wr=new PrintWriter(wordsDic,"utf-8")
    //词典表
    val words=Set[String]()

    val data=sc.textFile(inFile).map(line=> {
      val temp = line.split("#")
      val articleMap = if (temp.length > 1) {
        val content=temp(1).trim
        countWord(content.split(" "))
      } else {
        HashMap("开始"->0)
      }
      articleMap
    }).collect()

    println("[数据读取OVER！！！！！！！！！！]")
    var count =0
    //提取重要的生词，并将生词加入到words
    data.foreach(line =>{
      println("还剩下 "+(data.size-count))
      count +=1
      line.foreach(word=>{
        val tf_idf=Util.getTf_Idf3(word._1,line,data)
        //设置一个阈值，将tf_idf>阈值的关键词提取出来，并加入到词典
        println("    || "+word._1+": "+tf_idf)
        if(tf_idf>=0.02) words += word._1
      })
    })

    //遍历并写出
    words.foreach(word=>wr.append(word+"\n").flush())
    words
  }

  /**
    * LDA 预处理 ：以字典长度为维度，因此维度极高
    *
    * @param inFile
    * @param outFile  格式为：每篇占一行，每行都是tf值，空格分隔
    * @param wordsDic
    * @author  zhangxin
    */
  def textPre2VSM(inFile:String,outFile:String,wordsDic:String,sc:SparkContext): Unit ={
    val wr=new PrintWriter(outFile,"utf-8")

    //①词典表
//    val words=allWordsDic(inFile,wordsDic,sc)
    val words=Source.fromFile(wordsDic).getLines().toArray

//    val file=sc.textFile(inFile).map(_.split("#")(1).trim.split(" "))

    val file2=sc.textFile(inFile).map(line=> {
      val temp = line.split("#")
      val contentArr = if (temp.length > 1) {
        temp(1).trim.split(" ")
      } else {
        Array("开始")
      }
      contentArr
    })


    //②得到每篇文章的词频map
    val data=file2.map(line=>{
      val articleMap=countWord(line)
      articleMap
    }).collect()

    //对每篇文章，以词典表进行遍历，构建VSM模型，并写出
    data.foreach(line=>{
      words.foreach(word=>{
        var n=0;
        if(line.keySet.contains(word)) {n=line(word)}
        print(n+" ")
        wr.append(n.toString+" ")
        wr.flush()
      })
      println()
      wr.append("\n")
      wr.flush()
    })
  }

  /**
    * LDA 预处理 ：每一行无0值，整个表示模型维度低
    *
    * @param inFile
    * @param outFile  格式为：每篇占一行，每行都是tfidf值，空格分隔
    */
  def textPreSimple(inFile:String,outFile:String,sc:SparkContext): Unit ={
    val wr=new PrintWriter(outFile,"utf-8")
    var count =0
    val data=sc.textFile(inFile).map(line=>{
      val temp=line.split("#")
      println(temp(0))
      val articleMap=countWord(temp(1).trim.split(","))
      articleMap
    }).collect()

    println("[数据读取OVER！！！！！！！！！！]")
    data.foreach(line =>{
      count +=1
      println("[文章编号] "+count+"******************************************")
      line.foreach(word=>{
        val tf_idf=Util.getTf_Idf3(word._1,line,data)
        wr.append(tf_idf+" ")
        wr.flush()
      })
      wr.append("\n")
      wr.flush()
    })
  }
  /**
    * 对每篇文章进行词频统计
    *
    * @param file
    * @return
    */
  def countWord(file:Array[String]):HashMap[String,Int]={
    var wordmap=new HashMap[String,Int]
    file.foreach(word=>{
      if(wordmap.keySet.contains(word)) {
        val count=wordmap(word)
        wordmap(word) = count+ 1
      }else{
        wordmap +=(word->1)
      }
    })
    wordmap
  }

  //**********************************************************************************************
  //**********************************************************************************************

  //【处理码农场的数据方法】

  //批量数据-> 一个txt
  def textHankcs2Txt(inFile:String,outFile:String): Unit ={
    val wr=new PrintWriter(outFile,"utf-8")
    val fileList=new File(inFile).listFiles()
    var count=0
    fileList.foreach(file=>{
      count +=1
      println("[还剩下]  "+(fileList.size-count))
      val content=Source.fromFile(file).getLines().toArray.mkString
      val label=file.getName.substring(0,file.getName.indexOf("_"))
      wr.append(label+"#"+content+"\n")
      wr.flush()
    })
  }

}
