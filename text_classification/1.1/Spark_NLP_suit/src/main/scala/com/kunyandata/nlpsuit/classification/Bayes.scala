package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io._

import com.kunyandata.nlpsuit.util.WordSeg
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.classification.NaiveBayesModel
import scala.io.Source


object Bayes extends App{

  /**
    * 初始化模型，将本地序列化的模型都反序列化到内存中。
    *
    * @param path 保存模型的路径
    * @return 返回一个嵌套Map，第一层key是行业名称，第二层key是模型名称。
    */
  def initModel(path: String): Map[String, Map[String, Any]] = {
    // 读取本地保存的模型
    val modelKey = Array("tfModel", "idfModel", "chiSqSelectorModel", "nbModel")
    val fileList = new File(path)
    val cateList = fileList.listFiles()
    var resultMap:Map[String, Map[String, Any]] = Map()
    var tempMap:Map[String, Any] = Map()
    cateList.foreach(cate => {
      val tempCatePath = path + "/" + cate.getName
      modelKey.foreach(modelName => {
        val tempModelPath = tempCatePath + "/" + modelName
        val tempModelInput = new ObjectInputStream(new FileInputStream(tempModelPath))
        tempMap += (modelName -> tempModelInput.readObject())
      })
      resultMap += (cate.getName -> tempMap)
    })
    resultMap
  }

  /**
    * 获取停用词典
    *
    * @param path 停用词典存放路径
    * @return 返回一个Array[String]的停用词表
    */
  def getStopWords(path: String): Array[String] = {
    val stopWords = Source.fromFile(path).getLines().toArray
    stopWords
  }

  /**
    * 行业类别预测
    *
    * @param content 正文字符串
    * @param models 模型Map，由intiModel方法提供
    * @return 返回一个字符串，包含了行业名称，例子：“银行,保险”
    */
  def industryPredict(content: String, models: Map[String, Map[String, Any]], stopWords: Array[String]): String = {
    val wordSegJson = WordSeg.splitWord(content, 1)
    val wordSeg = WordSeg.getWords(wordSegJson)

    // 去除停用词
    val wordSegNoStop = WordSeg.removeStopWords(wordSeg, stopWords)
    val classificationResult = models.keys.map(key => {
      val tfModel = models(key)("tfModel").asInstanceOf[HashingTF]
      val tf = tfModel.transform(wordSegNoStop)

      // idf
      val idfModel = models(key)("idfModel").asInstanceOf[IDFModel]
      val tfidf = idfModel.transform(tf)

      // chisqselector
      val chisqSelector = models(key)("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
      val selectedFeatures = chisqSelector.transform(tfidf)

      //
      val nbModel = models(key)("nbModel").asInstanceOf[NaiveBayesModel]
      val prediction = nbModel.predict(selectedFeatures)
      if (prediction == 1.0) key
    })
    classificationResult.mkString(",")
  }
}
