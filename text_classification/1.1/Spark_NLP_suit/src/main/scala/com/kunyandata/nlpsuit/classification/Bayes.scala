package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io._
import com.kunyandata.nlpsuit.util.TextPreprocessing
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.classification.NaiveBayesModel

import scala.io.Source


object Bayes {

  /**
    * 初始化模型，将本地序列化的模型都反序列化到内存中。
    *
    * @param path 保存模型的路径
    * @return 返回一个嵌套Map，第一层key是行业名称，第二层key是模型名称。
    */
  def initModel(defaultFS: String, path: String): Map[String, Map[String, Serializable]] = {

    //读取hdfs上保存的模型
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", defaultFS)
    val fs = FileSystem.get(hdfsConf)
    val fileList = fs.listStatus(new Path(path)).map(_.getPath.toString)
    val result = fileList.map(file => {
      val indus = file.replaceAll(".models", "").replaceAll(defaultFS, "").replaceAll(path, "")
      val temp = new ObjectInputStream(fs.open(new Path(file))).readObject()
      val modelMap = temp.asInstanceOf[Map[String, Serializable]]
      (indus, modelMap)
    }).toMap
    result
  }

  def initModel(path: String): Map[String, Map[String, Serializable]] = {

    //读取本地保存的模型
    val fileList = new File(path).listFiles().map(_.getName.toString)
    val result = fileList.map(file => {
      println(file)
      val indus = file.replaceAll(".models", "")
      println(indus)
      val temp = new ObjectInputStream(new FileInputStream(path + file))
      val modelMap = temp.asInstanceOf[Map[String, Serializable]]
      (indus, modelMap)
    }).toMap
    result
  }
  /**
    * 获取停用词典
    *
    * @param path 停用词典存放路径
    * @return 返回一个Array[String]的停用词表
    */
  def getStopWords(path: String): Array[String] = {
    Source.fromFile(path).getLines().toArray
  }

  /**
    * 获取停用词（hfds）
    *
    * @param sc SparkContext
    * @param path hdfs uri
    * @return 返回一个Array[String]的停用词表
    */
  def getStopWords(sc: SparkContext, path: String): Array[String] = {
    sc.textFile(path).collect()
  }

  /**
    * 行业类别预测
    *
    * @param wordSegNoStop 经过分词和去停用词处理的文本
    * @param indusModels 模型Map，由intiModel方法提供
    * @return 返回一个字符串，包含了行业名称，例子：“银行,保险”
    */
  def indusPredict(wordSegNoStop: Array[String], indusModels: Map[String, Map[String, AnyRef]]): String = {
    val classificationResult = indusModels.keys.map(key => {
      val prediction = indusModels(key)("nbModel").asInstanceOf[NaiveBayesModel]
        .predict(indusModels(key)("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
          .transform(indusModels(key)("idfModel").asInstanceOf[IDFModel]
            .transform(indusModels(key)("tfModel").asInstanceOf[HashingTF]
              .transform(wordSegNoStop))))
      if (prediction == 1.0) key
    })
    classificationResult.filter(_ != ()).mkString(",")
  }

  /**
    * 概念板块类别预测
    *
    * @param wordSegNoStop 经过分词和去停用词处理的文本
    * @param sectionModels 模型Map，由intiModel方法提供
    * @return 返回一个字符串，包含了概念板块的名称，例子：“P2P, 4G”
    */
  def sectionPredict(wordSegNoStop: Array[String], sectionModels: Map[String, Map]): String = {
    null
  }
}
