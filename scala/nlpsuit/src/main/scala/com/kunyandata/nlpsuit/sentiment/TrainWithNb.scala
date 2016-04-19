package com.kunyandata.nlpsuit.sentiment

import java.io.File
import com.kunyandata.nlp.classification.TrainingProcess
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by zx on 2016/3/28.
  */
object TrainWithNb {

  /**
    * 基于RDD的贝叶斯训练
    * 备注：仅训练测试，模型不保存
    * @param sc
    * @param filepath  数据集文件路径(经过预处理的数据集)
    */
  def nbTrain(sc:SparkContext,filepath:String): Unit ={
    val trainData=sc.textFile(filepath)

    //基于RDD的训练流程
    val dataRDD=trainData.map(line =>{
      val temp = line.split("#")
      (temp(0).toDouble, temp(1).split(","))
    })

    println("数据载入结束")
    //按照8:2的比例随机划分数据集
    val dataSets = dataRDD.randomSplit(Array(0.8, 0.2), seed = 2016L)
    val train=dataSets(0).cache()
    val test=dataSets(1).cache()

    val result=TrainingProcess.trainingProcessWithRDD(train,test, Array(0), Array(1000),10000)
    println(result)
    sc.stop()
  }

  /**
    * 基于RDD的贝叶斯训练,并保存模型到默认的hdfs "hdfs://222.73.57.12:9000"
    * @param sc
    * @param filepath 数据集路径
    * @param indus  模型名称（行业名称）
    * @param minDF 最小文档阈值
    * @param topFeat 最大特征空间值
    */
  def nbTrainToHdfs(sc:SparkContext,filepath:String,indus: String,minDF:Int,topFeat:Int): Unit ={
    val hdfsAddress="hdfs://222.73.57.12:9000"
    val trainData=sc.textFile(filepath)
    //基于RDD的训练流程
    val dataRDD=trainData.map(line =>{
      val temp = line.split("#")
      (temp(0).toDouble, temp(1).split(","))
    })
    val result=TrainingProcess.outPutModels(hdfsAddress,filepath,dataRDD,indus,minDF,topFeat)
    println(result)
    sc.stop()
  }

  /**
    * 基于RDD的贝叶斯训练，并保存模型到本地路径
    * 备注：数据不划分，全部用于train
    * @param sc
    * @param filepath 数据集路径
    * @param outPath 模型输出路径 如E:/svmmodels/
    * @param indus  模型名称（行业名称）
    * @param minDF 最小文档阈值
    * @param topFeat 最大特征空间值
    */
  def nbTrainToLocal(sc:SparkContext,filepath:String,outPath:String,indus: String,minDF:Int,topFeat:Int): Unit ={
    val trainData=sc.textFile(filepath)
    //基于RDD的训练流程
    val dataRDD=trainData.map(line =>{
      val temp = line.split("#")
      (temp(0).toDouble, temp(1).split(","))
    })
    val result=TrainingProcess.outPutModels(outPath,dataRDD,indus,minDF,topFeat)
    println(result)
    sc.stop()
  }

  /**
    * 基于网格参数寻优的训练
    * @param sc
    * @param filepath  数据集路径
    */
  def tuneParasTrain(sc:SparkContext,filepath:String): Unit ={
    val trainData=sc.textFile(filepath)
    val dataRDD=trainData.map(line=>{
      val temp=line.split("#")
      (temp(0).toDouble, temp(1).split(","))  //K_V二元组
    })

    //数据分割
    val dataSets = dataRDD.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
    val dataSet = Array(
//      Map[Array[String],Array[String]](dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)
      Map("train" ->dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(4)), "test" -> dataSets(3)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(2)),
      Map("train" -> dataSets(0).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(1)),
      Map("train" -> dataSets(1).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(0))
    )

    TrainingProcess.tuneParas(dataSet,Array(1),Array(500),"Testzx")
  }
}
