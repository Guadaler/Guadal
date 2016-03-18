package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io.{ObjectInputStream, FileInputStream, FileOutputStream, ObjectOutputStream}
import java.util.Date

import org.apache.spark.ml.Pipeline
import scala.io.Source
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.SparseVector
import com.kunyandata.nlpsuit.util.WordSeg


object TrainingProcess extends App{

  case class dataForamt(id: String , category: String, content:String, label:Double)
  val a = Array("你 好 吗？")
  val conf = new SparkConf().setAppName("mltest").setMaster("local")
  val sc = new SparkContext(conf)
  val dataSet = Source.fromFile("D:\\wordseg_881155").getLines().toArray
  val time1 = new Date().getTime
  val sqlContext = new SQLContext(sc)
  val time2 = new Date().getTime
  val b = sqlContext.createDataFrame(a.map(line => {
    dataForamt("1", "1", line, 0.0)
  })).toDF()
  val time3 = new Date().getTime
  val wordDataFrame = sqlContext.createDataFrame(dataSet.map(line =>{
    val temp = line.split("\t")
    var lab = 0.0
    dataForamt(temp(0), temp(1), temp(2), label = if(temp(1) == "881155") 1.0 else 0.0)
  })).toDF
  
  println(time2-time1)
  println(time3-time1)

  wordDataFrame.show()

  // 拆分训练集和测试集
  val Array(trainData, testData) = wordDataFrame.randomSplit(Array(0.7, 0.3), seed = 11L)

  val stopWords = Source.fromFile("D:\\WorkSpace\\Python_WorkSpace" +
    "\\Python_classification\\dicts\\stop_words_CN").getLines().toArray

  // token
  var tokenizer = new Tokenizer()
    .setInputCol("content")
    .setOutputCol("words")

  // 去除停用词
  val stopWordsRemover = new StopWordsRemover()
    .setStopWords(stopWords)
    .setInputCol("words")
    .setOutputCol("filtered")
  //  val worddf = stopWordsRemover.transform(wordDataFrame)

  // 构建向量空间模型
  val cvModel = new CountVectorizer()
    .setInputCol(stopWordsRemover.getOutputCol)
    .setOutputCol("rawFeatures")

  // 计算idf值，并根据向量空间模型中的tf值获得tfidf
//  val idfModel = new IDF()
//    .setInputCol(cvModel.getOutputCol)
//    .setOutputCol("features")
  val inppput = new ObjectInputStream(new FileInputStream("D:/idfModel"))
  val idfModel = inppput.readObject().asInstanceOf[IDF]

  val featureSelector = new ChiSqSelector()
    .setNumTopFeatures(500)
    .setFeaturesCol(idfModel.getOutputCol)
    .setLabelCol("label")
    .setOutputCol("selectedFeatures")

  val vectorSpacePipline = new Pipeline()
    .setStages(Array(tokenizer, stopWordsRemover, cvModel, idfModel))
  val vectorSpacePiplineM = vectorSpacePipline.fit(trainData)
  val trainCM = vectorSpacePiplineM.transform(trainData)
  val testCM = vectorSpacePiplineM.transform(testData)
  trainCM.show()
  testCM.show()

  // 转换数据类型
  val train = trainCM.select("label", "features").map(row => {
    LabeledPoint(row.getDouble(0), row.getAs[SparseVector](1))
  })

  val test = testCM.select("label", "features").map(row => {
    LabeledPoint(row.getDouble(0), row.getAs[SparseVector](1))
  })

  // 朴素贝叶斯
  val nBModel = NaiveBayes.train(train, lambda = 1.0, modelType = "multinomial")
  nBModel

  val predictionAndLabels = test.map {case LabeledPoint(label, features) =>
    val prediction = nBModel.predict(features)
    (prediction, label)
  }

  //  训练模型SVM
//  val num = 200
//  val svmModel = SVMWithSGD.train(train, num)
//  val predictionAndLabels = test.map {case LabeledPoint(label, features) =>
//    val prediction = svmModel.predict(features)
//    (prediction, label)
//  }

  val metrics = new MulticlassMetrics(predictionAndLabels)
  println("Confusion matrix:")
  println(metrics.confusionMatrix)

  // Precision by label
  val labels = metrics.labels
  labels.foreach { l =>
    println(s"Precision($l) = " + metrics.precision(l))
  }

  // Recall by label
  labels.foreach { l =>
    println(s"Recall($l) = " + metrics.recall(l))
  }

  // False positive rate by label
  labels.foreach { l =>
    println(s"FPR($l) = " + metrics.falsePositiveRate(l))
  }

  // F-measure by label
  labels.foreach { l =>
    println(s"F1-Score($l) = " + metrics.fMeasure(l))
  }




  sc.stop()
}
