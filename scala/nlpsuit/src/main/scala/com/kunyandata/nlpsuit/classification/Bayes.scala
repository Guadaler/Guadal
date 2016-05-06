package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io._
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vector

import scala.collection.Map
import scala.io.Source


object Bayes {

  /**
    * 初始化模型，将本地序列化的模型都反序列化到内存中。
    *
    * @param path 保存模型的local路径
    * @return 返回一个嵌套Map，第一层key是类别名称，第二层key是模型名称。
    * @author QQ
    */
  def initModels(path: String): Map[String, Map[String, Map[String, Serializable]]] = {

    //读取本地保存的模型
    val indusList = new File(path).listFiles().map(_.getName)
    val result = indusList.map(cateName => {
      val tmpModelsPath = path + "/" + cateName
      val fileList = new File(tmpModelsPath).listFiles().map(_.getName)
      val resultTemp = fileList.map(file => {
        val category = file.replaceAll(".models", "")
        val tmpModelPath = tmpModelsPath + "/" + file
        val temp = new ObjectInputStream(new FileInputStream(tmpModelPath)).readObject()
        val modelMap = temp.asInstanceOf[Map[String, Serializable]]
        (category, modelMap)
      }).toMap
      (cateName, resultTemp)
    }).toMap
    result
  }

  /**
    * 获取本地路径下的分类实体词典
    *
    * @param path 存放分类实体词典的路径
    * @return 返回一个嵌套map
    * @author QQ
    */
  def initGrepDicts(path: String): Map[String, Map[String, Array[String]]] = {

    val stockDict = Source.fromFile(path + "/stock_words.words").getLines().toArray.map(line => {
      val Array(stockCode, words) = line.split("\t")
      (stockCode, words.split(","))
    }).toMap
    val sectionDict = Source.fromFile(path + "/section_words.words").getLines().toArray.map(line => {
      val Array(stockCode, words) = line.split("\t")
      (stockCode, words.split(","))
    }).toMap
    Map("stockDict" -> stockDict, "setcionDict" -> sectionDict)
  }

  /**
    * 获取停用词典
    *
    * @param path 停用词典存放路径
    * @return 返回一个Array[String]的停用词表
    * @author QQ
    */
  def getStopWords(path: String): Array[String] = {
    Source.fromFile(path).getLines().toArray
  }

  /**
    * 行业类别预测
    *
    * @param wordSegNoStop 经过分词和去停用词处理的文本
    * @param indusModels 模型Map，由intiModel方法提供
    * @return 返回一个字符串，包含了行业名称，例子：“银行,保险”
    * @author QQ
    */
  def indusPredict(wordSegNoStop: Array[String], indusModels: Map[String, Map[String, Serializable]]): String = {
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
    * @author QQ
    */
  def sectionPredict(wordSegNoStop: Array[String], sectionModels: Map[String, Map[String, Serializable]]): String = {
    val prediction = sectionModels("概念板块")("nbModel").asInstanceOf[NaiveBayesModel]
      .predictProbabilities(sectionModels("概念板块")("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(sectionModels("概念板块")("idfModel").asInstanceOf[IDFModel]
          .transform(sectionModels("概念板块")("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))
//    getTopLabels(prediction).mkString(",")
    null
  }

  /**
    * 预测，并返回一个tuple,其中包括3个大类的分类结果。
    *
    * @param wordSegNoStop 分词之后的结果，为一个字符串数组
    * @param models 初始化之后的模型
    * @return 返回一个tuple，里面为3个字符串，分别代表股票、行业、概念板块的分类信息
    * @author QQ
    */
  def predict(wordSegNoStop: Array[String],
              models: Map[String, Map[String, Map[String, Serializable]]],
              classifyDicts: Map[String, Map[String, Array[String]]]): (String, String, String) = {
    val mlresult = models.map(model => {
      val resultTmp = indusPredict(wordSegNoStop, model._2)
      (model._1, resultTmp)
    })
    val grepResult = classifyDicts.map(dict =>{
      val resultTmp = Regular.grep(wordSegNoStop, dict._2)
      (dict._1, resultTmp)
    })
    (grepResult("stockDict"), mlresult("indusModels"), grepResult("setcionDict"))
  }
}
