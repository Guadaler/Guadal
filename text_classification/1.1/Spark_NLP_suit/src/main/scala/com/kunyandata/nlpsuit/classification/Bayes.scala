package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io.{FileOutputStream, ObjectOutputStream}

import com.kunyandata.nlpsuit.util.WordSeg
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.classification.NaiveBayesModel

import scala.io.Source


object Bayes {

  def initModel(sc: SparkContext): Map[String, Map[String, Any]] = {
    // 读取本地保存的模型
    val modelPath = ""
    NaiveBayesModel.load(sc, modelPath)



    // 读取训练集，并运行训练集
    null
  }

  def trainDataLoad() = {
    val trainDataPath = ""

  }

  def predict(content: String, textFormatModels: Map[String, Any], classModel: Map[String, NaiveBayesModel]) = {
    val wordSegJson = WordSeg.splitWord(content, 1)
    val wordSeg = WordSeg.getWords(wordSegJson)
    val stopWords = Source.fromFile("D:\\WorkSpace\\Python_WorkSpace" +
      "\\Python_classification\\dicts\\stop_words_CN").getLines().toArray

    // 去除停用词
    val wordSegNoStop = WordSeg.removeStopWords(wordSeg, stopWords)

    // tf
    val tfModel = new HashingTF(1000000)
    val tf = tfModel.transform(wordSegNoStop)

    // idf
    val idfModel = textFormatModels("idfModel").asInstanceOf[IDFModel]
    val tfidf = idfModel.transform(tf)

    // chisqselector
    val chisqSelector = textFormatModels("chisqSelector").asInstanceOf[ChiSqSelectorModel]
    val selectedFeatures = chisqSelector.transform(tfidf)

    //
  }
}
