package com.kunyandata.nlpsuit.sentiment

import java.io.File

import com.kunyandata.nlpsuit.classification.TrainingProcess
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by zx on 2016/3/28.
  */
object NB_train extends App{

  val conf=new SparkConf().setAppName("test").setMaster("local")
  val sc=new SparkContext(conf)
  val trainData1=sc.textFile("text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\sourcedata\\traindata_200.txt")
  val trainData2=sc.textFile("D:\\000_DATA\\out\\【3842】labelNum_content.txt")
  val trainData0=sc.textFile("E:\\data\\textSeg_content(1_ansj_6071).txt")
  val trainData3=sc.textFile("E:\\data\\textSeg (第三次程序运行结果_6071_ansj).txt")
  val trainData4=sc.textFile("D:\\000_DATA\\out\\【3842】labelNum_content_noNeu.txt")  //积极和消极
  val trainData5=sc.textFile("D:\\000_DATA\\out\\【第二次标注程序结果】\\【3842】【Neu+pos】labelNum_content.txt")  //中性 +积极
  val trainData6=sc.textFile("D:\\000_DATA\\out\\【第三次标注程序结果】\\【2000+2000】【pro】textSeg_content.txt")  //中性 +综合
  val trainData8=sc.textFile("D:\\000_DATA\\out\\【第三次标注程序结果】\\【1000+1000】【pro】textSeg_content.txt")  //pos +neu
  val trainData9=sc.textFile("D:\\000_DATA\\out\\【第三次标注程序结果】\\【500+500】【pro】textSeg_content.txt")  //中性 +综合
  val trainData10=sc.textFile("D:\\000_DATA\\out\\【第三次标注程序结果】\\【neg+1000+1000】textSeg_content.txt")  //中性 +综合

  //------------------------------------------
  val outPath_content_4="D:\\000_DATA\\out\\【第四次标注程序结果】\\【2000+1600+1700】_pre_textSeg_content.txt"
  val outPath_content_F_4="D:\\000_DATA\\out\\【第四次标注程序结果】\\F_2000_textSeg_content.txt"
  val outPath_content_S_4="D:\\000_DATA\\out\\【第四次标注程序结果】\\S_1600_textSeg_content.txt"

  //------------------------------------------
  val outPath_content="D:\\000_DATA\\out\\【第五次标注程序结果】\\【1700+1500+1700】textSeg_content.txt"
  val outPath_content_F="D:\\000_DATA\\out\\【第五次标注程序结果】\\F_1700_textSeg_content.txt"
  val outPath_content_S="D:\\000_DATA\\out\\【第五次标注程序结果】\\S_1600_textSeg_content.txt"

  val trainData=sc.textFile(outPath_content_S)  //中性 +综合


  //基于RDD的训练流程
// val t=Array[String]("test1","test2")
//  val dataRDD=trainData.map(line =>{
//    val temp = line.split("#")
//    if(temp.length >1){
//      (temp(0).toDouble, temp(1).split(","))
//    }else{
//      println("空值"+line)
//      (temp(0).toDouble, t)
//    }
//  })

  //基于RDD的训练流程
  val dataRDD=trainData.map(line =>{
    val temp = line.split("#")
    (temp(0).toDouble, temp(1).split(","))
  })

  println("数据载入结束")
  //按照8:2的比例随机划分数据集
  val dataSets = dataRDD.randomSplit(Array(0.8, 0.2), seed = 2016L)

  val dataSet=Seq(
    Map("train" -> dataSets(0), "test" -> dataSets(1))
  )

  val result = TrainingProcess.trainingProcessWithRDD(dataSet(0)("train"), dataSet(0)("test"), 0, 500,true)
  println(result)
  sc.stop()
}
