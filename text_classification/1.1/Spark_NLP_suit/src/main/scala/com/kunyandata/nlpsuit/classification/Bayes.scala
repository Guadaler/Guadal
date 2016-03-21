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

  val kk = initModel("D:/test")
  println(kk.keys)
  println(kk("银行").keys)
  println(kk)


  def predict(content: String, models: Map[String, Map[String, Any]]):String = {
    val wordSegJson = WordSeg.splitWord(content, 1)
    val wordSeg = WordSeg.getWords(wordSegJson)
    val stopWords = Source.fromFile("D:\\WorkSpace\\Python_WorkSpace" +
      "\\Python_classification\\dicts\\stop_words_CN").getLines().toArray

    // 去除停用词
    val wordSegNoStop = WordSeg.removeStopWords(wordSeg, stopWords)
    val classificationResult = models.keys.map(key => {
      val tfModel = models(key)("tfModel").asInstanceOf[HashingTF]
      val tf = tfModel.transform(wordSegNoStop)

      // idf
      val idfModel = models(key)("idfModel").asInstanceOf[IDFModel]
      val tfidf = idfModel.transform(tf)

      // chisqselector
      val chisqSelector = models(key)("chisqSelector").asInstanceOf[ChiSqSelectorModel]
      val selectedFeatures = chisqSelector.transform(tfidf)

      //
      val nbModel = models(key)("nbModel").asInstanceOf[NaiveBayesModel]
      val prediction = nbModel.predict(selectedFeatures)
      if (prediction == 1.0) key else null
    })
    classificationResult.mkString(",")
  }
}
