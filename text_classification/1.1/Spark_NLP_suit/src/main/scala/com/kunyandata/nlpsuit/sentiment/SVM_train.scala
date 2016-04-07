package com.kunyandata.nlpsuit.sentiment

import java.io.{PrintWriter, File}
import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by Administrator on 2016/4/5.
  */
object SVM_train extends App{
//  var filepath="D:\\000_DATA\\out\\【第三次标注程序结果】\\【neg+1000+1000】textSeg_content.txt"
//  var filepath="D:\\000_DATA\\out\\【第三次标注程序结果】\\【500+500】【pro】【2】textSeg_content.txt"

//  var filepath="D:\\000_DATA\\out\\【第三次标注程序结果】\\【2000+2000】【pro】textSeg_content.txt"
  var filepath="D:\\000_DATA\\out\\【第三次标注程序结果】\\【1000+1000】【pro】textSeg_content.txt"

  val libsvmpath="D:\\000_DATA\\out\\SVM\\libsvm_1000(23).txt"
//  libsvm_seg(filepath, libsvmpath)

  train(libsvmpath)

  /**
    * 对已经分词去停格式化过的文本处理成libsvm格式
    * 格式: label#content_seg
    *
    * @param filepath
    * @param libsvmPath
    */
  def libsvm_seg(filepath:String,libsvmPath:String): Unit ={
    val conf=new SparkConf().setAppName("test").setMaster("local")
    val sc=new SparkContext(conf)

    //    var file_map=IO.readfile2(filepath);
    //    var word_map=new util.HashMap[File,util.HashMap[String,Int]] //所有文章词对  文章File：[词：Tf-IDF值]
    var word_map=new util.HashMap[String,util.HashMap[String,Int]] //所有文章词对  文章编号：[词：Tf-IDF值]
    var wordsDict=new util.HashMap[String,Int]  //词典 编号:词

    //初始化分词组件
    Analyzer.add_dic("text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\senti_dict\\user_dict.txt",sc)

    var countNum=0  //文章编号
    for(line <-Source.fromFile(new File(filepath)).getLines()){
      countNum +=1
      val label=line.substring(0,line.indexOf("#"))
      val labelNum=label match{
        case "2" => 0
        case "3" => 1

        case "1" => 0
        case "4" => 1
      }
      val content_seg=line.substring(line.indexOf("#")+1,line.length)

      var words_fre_map=new util.HashMap[String,Int]()  //词 词频
      val content_array=content_seg.split(",")
      for(term <-content_array){

        Analyzer.addMap(words_fre_map,term)
      }

      //      var onefile_content=Analyzer.removeUsenelss(result)
      //      Analyzer.add2wordsDict(wordsDict,onefile_content)

      Analyzer.add2wordsDict(wordsDict,words_fre_map)
      word_map.put(labelNum+"#"+countNum,words_fre_map)
    }

    //word_map => word_map_tfidf
    //         => libsvm
    //         => outPath

    var writer2=new PrintWriter(libsvmPath)

    var it2=word_map.keySet().iterator()
    var count=0
    while(it2.hasNext){
      var artNum=it2.next()
      var label=artNum.substring(0,artNum.indexOf("#"))
      var num=artNum.substring(artNum.indexOf("#")+1,artNum.length)


      writer2.append(label+" ")

      val keys =ListBuffer[Int]()
      var keys_value=new util.HashMap[Integer, Double];

      var onefile=word_map.get(artNum)
      var it3=onefile.keySet().iterator()
      while(it3.hasNext){
        var word=it3.next()
        var wordNum=wordsDict.get(word);
        var tf_idf=Util.getTf_Idf2(word,onefile,word_map);

        if(!keys.contains(wordNum)){
          keys.append(wordNum)
          keys_value.put(wordNum,tf_idf);
        }else{
//          println("有！！！！！！！！！！"+word+"  "+wordNum+" "+tf_idf)
        }

      }

      var key=keys.sorted;
      for(i <- key){
        writer2.append(i.toString+":"+keys_value.get(i)+" ")
      }
      writer2.println();
      writer2.flush();

      count +=1
      println("还有："+(word_map.size()-count))
    }
    writer2.flush();
    writer2.close();
  }


  def train(libfile:String): Unit ={
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //spark程序入口
    var conf=new SparkConf().setMaster("local").setAppName("testSvm")
    var sc=new SparkContext(conf)

    System.setProperty("hadoop.home.dir", "D:/hadoop");  //报Hadoop的错误

    var data=MLUtils.loadLibSVMFile(sc,libfile)
    var splits=data.randomSplit(Array(0.8,0.2))
    var train=splits(0).cache()
    var test=splits(1).cache()
    val itraterNum=100

    //训练
    var svmtest=new SVMWithSGD()
    var model=svmtest.run(train)

    //  预测
    val scoreandLabels=test.map{point=>
      var score=model.predict(point.features)
      (score,point.label)
    }

    //性能评价
    var metrics=new BinaryClassificationMetrics(scoreandLabels)
    var auROC=metrics.areaUnderROC()
    println("Under ROC=: "+auROC)

    //统计分类错误
    val trainErr = scoreandLabels.filter(r => r._1 != r._2).count.toDouble / data.count
    println("Training Error = " + trainErr)

    //计算模型精度
    val metrics2 = new MulticlassMetrics(scoreandLabels)
    val precision = metrics2.precision
    println("precision:" + precision)

    sc.stop()
  }
}
