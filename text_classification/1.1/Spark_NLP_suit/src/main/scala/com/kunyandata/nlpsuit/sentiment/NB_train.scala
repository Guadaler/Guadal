package com.kunyandata.nlpsuit.sentiment

import com.kunyandata.nlpsuit.classification.TrainingProcess
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2016/3/28.
  */
object NB_train extends App{

  val conf=new SparkConf().setAppName("test").setMaster("local")
  val sc=new SparkContext(conf)
  val trainData=sc.textFile("text_classification\\1.1\\Spark_NLP_suit\\src\\test\\resources\\sentiment_data\\sourcedata\\traindata_200.txt")

  //基于RDD的训练流程
  val dataRDD=trainData.map(line =>{
    var temp=line.split("#")
    Seq(temp(0), temp(1).toSeq)
  })

  //按照8:2的比例随机划分数据集
  val dataSets = dataRDD.randomSplit(Array(0.8, 0.2), seed = 2016L)
  val dataSet=Seq(
    Map("train" -> dataSets(0), "test" -> dataSets(1))
  )

//  val result = TrainingProcess.trainingProcessWithRDD(dataSet(0)("train"), dataSet(0)("test"), 0, 500,false)
//  println(result)
  sc.stop()
}
