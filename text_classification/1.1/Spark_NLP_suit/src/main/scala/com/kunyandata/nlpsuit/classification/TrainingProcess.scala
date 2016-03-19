package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io.{ObjectInputStream, FileInputStream, FileOutputStream, ObjectOutputStream}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.mllib.feature
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import com.kunyandata.nlpsuit.util.WordSeg
import org.apache.spark.sql._
import org.apache.spark.sql.types._


private object TrainingProcess extends App{

  def trainingProcessWithRDD(train: RDD[Seq[Object]], test: RDD[Seq[Object]]) = {
    // 构建hashingTF模型，同时将数据转化为LabeledPoint类型
    val hashingTFModel = new feature.HashingTF()
    val trainTFRDD = train.map(line => {
      val temp = hashingTFModel.transform(line(2).asInstanceOf[Seq[String]])
      (if(line(1).asInstanceOf[String] == "881155") 1.0 else 0.0, temp)
    })

    // 计算idf
    val idfModel = new feature.IDF(2).fit(trainTFRDD.map(line => {line._2}))
    val labeedTrainTfIdf = trainTFRDD.map( line => {
      val temp = idfModel.transform(line._2)
      LabeledPoint(line._1, temp)
    })

    val idfModelOutput = new ObjectOutputStream(new FileOutputStream("D:/idfModel"))
    idfModelOutput.writeObject(idfModel)

    // 卡方降维特征选择器
    val chiSqSelectorModel = new feature.ChiSqSelector(50).fit(labeedTrainTfIdf)
    val selectedTrain = labeedTrainTfIdf.map(line => {
      val temp = chiSqSelectorModel.transform(line.features)
      LabeledPoint(line.label, temp)
    })

    val chiSqSelectorModelOutput = new ObjectOutputStream(new FileOutputStream("D:/chiSqSelectorModel"))
    chiSqSelectorModelOutput.writeObject(chiSqSelectorModel)
    println("+++++++++++++++++++++++++++++++++++++++++++++特征选择结束++++++++++++++++++++++++++++++++++++++++++++++++++")
    // 创建贝叶斯分类器
    val nbModel = NaiveBayes.train(labeedTrainTfIdf, 1.0, "multinomial")

    // 根据训练集同步测试集特征
    val testRDD = test.map(line => {
      val temp = line(2).asInstanceOf[Seq[String]]
      val tempTf = hashingTFModel.transform(temp)
      val tempTfidf = idfModel.transform(tempTf)
      //    val tempSelected = chiSqSelectorModel.transform(tempTfidf)
      LabeledPoint(if(line(1).asInstanceOf[String] == "881155") 1.0 else 0.0, tempTfidf)
    })

    val predictionAndLabels = testRDD.map {line =>
      val prediction = nbModel.predict(line.features)
      (prediction, line.label)
    }

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
  }

  def trainingProcessWithDF(sc: SparkContext, train:RDD[Seq[Object]], test: RDD[Seq[Object]]) = {
    val sqlContext = new SQLContext(sc)
    val schema =
      StructType(
        StructField("id", StringType, nullable = false) ::
          StructField("category", StringType, nullable = false) ::
          StructField("content", ArrayType(StringType, containsNull = true), nullable = false) ::
          StructField("label", DoubleType, nullable = false) :: Nil)
    val dataDF = sqlContext.createDataFrame(train.map(line => {
      Row(line(0), line(1), line(2).asInstanceOf[Seq[String]].toArray, if(line(1) == "881155") 1.0 else 0.0)
    }), schema).toDF()

    val testDF = sqlContext.createDataFrame(test.map(line => {
      Row(line(0), line(1), line(2).asInstanceOf[Seq[String]].toArray, if(line(1) == "881155") 1.0 else 0.0)
    }), schema).toDF()

    // token
    val tokenizer = new Tokenizer()
      .setInputCol("content")
      .setOutputCol("words")
    // 去除停用词
    val stopWordsRemover = new StopWordsRemover()
      .setStopWords(stopWords)
      .setInputCol("content")
      .setOutputCol("filtered")
    //  val worddf = stopWordsRemover.transform(wordDataFrame)

    // 构建向量空间模型
//    val hashingTFModel = new HashingTF()
//      .setInputCol(stopWordsRemover.getOutputCol)
//      .setOutputCol("rawFeatures")
//      .setNumFeatures(50000)

    val cvModel = new CountVectorizer()
      .setInputCol(stopWordsRemover.getOutputCol)
      .setOutputCol("rawFeatures")

    // 计算idf值，并根据向量空间模型中的tf值获得tfidf
    val idfModel = new IDF()
      .setInputCol(cvModel.getOutputCol)
      .setOutputCol("features")
      .setMinDocFreq(0)

    val idfModel2 = new IDF()
      .setInputCol(cvModel.getOutputCol)
      .setOutputCol("features")
      .setMinDocFreq(2)

    //  val inppput = new ObjectInputStream(new FileInputStream("D:/idfModel"))
    //  val idfModel = inppput.readObject().asInstanceOf[IDF]

    val featureSelector = new ChiSqSelector()
      .setNumTopFeatures(500)
      .setFeaturesCol(idfModel.getOutputCol)
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val vectorSpacePipline = new Pipeline()
      .setStages(Array(stopWordsRemover, cvModel, idfModel, featureSelector))
    val vectorSpacePiplineM = vectorSpacePipline.fit(dataDF)
    val trainCM = vectorSpacePiplineM.transform(dataDF)
    val testCM = vectorSpacePiplineM.transform(testDF)

    val vectorSpacePipline2 = new Pipeline()
      .setStages(Array(stopWordsRemover, cvModel, idfModel2, featureSelector))
    val vectorSpacePiplineM2 = vectorSpacePipline2.fit(dataDF)
    val trainCM2 = vectorSpacePiplineM2.transform(dataDF)
    trainCM.show
    trainCM2.show

    // 转换数据类型
    val trainData = trainCM.select("label", "selectedFeatures").map(line => {
      LabeledPoint(line.getDouble(0), line.getAs[SparseVector](1))
    })

    val testData = testCM.select("label", "selectedFeatures").map(line => {
      LabeledPoint(line.getDouble(0), line.getAs[SparseVector](1))
    })

    val nbModel = NaiveBayes.train(trainData, 1.0, "multinomial")

    val predictionAndLabels = testData.map {line =>
      val prediction = nbModel.predict(line.features)
      (prediction, line.label)
    }

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
  }



  val conf = new SparkConf().setAppName("mltest").setMaster("local")
  val sc = new SparkContext(conf)


  val data = Source.fromFile("D:\\WorkSpace\\Spark_WorkSpace\\ein" +
    "\\text_classification\\1.1\\Spark_NLP_suit\\src\\main" +
    "\\resources\\train\\wordseg_881155").getLines().toArray

  // 获取停用词
  val stopWords = Source.fromFile("D:\\WorkSpace\\Spark_WorkSpace" +
    "\\ein\\text_classification\\1.1\\Spark_NLP_suit\\src\\main" +
    "\\resources\\dicts\\stop_words_CN").getLines().toArray

  // 基于RDD的模型训练流程
  val dataRDD = sc.parallelize(data.map(line => {
    val temp = line.split("\t")
    val removedStopWords = WordSeg.removeStopWords(temp(2).split(" "), stopWords)
    Seq(temp(0), temp(1), removedStopWords.toSeq)
  }))

  val Array(trainDataRDD, testDataRDD) = dataRDD.randomSplit(Array(0.7, 0.3))

//  trainingProcessWithRDD(trainDataRDD, testDataRDD)
  trainingProcessWithDF(sc, trainDataRDD, testDataRDD)



  // 数据转化为RDD[LabeledPoint]格式
//  val labeldTrainTfidf = trainTfIdfRDD.map(line =>{

//  })
//  val chiSqSelectorModel = new ChiSqSelector(50).fit()

//  基于dataframe的模型训练流程
//  val time1 = new Date().getTime
//  val sqlContext = new SQLContext(sc)
//  val time2 = new Date().getTime
//  val b = sqlContext.createDataFrame(a.map(line => {
//    dataForamt("1", "1", line, 0.0)
//  })).toDF()
//  val time3 = new Date().getTime
//  val wordDataFrame = sqlContext.createDataFrame(dataSet.map(line =>{
//    val temp = line.split("\t")
//    var lab = 0.0
//    dataForamt(temp(0), temp(1), temp(2), label = if(temp(1) == "881155") 1.0 else 0.0)
//  })).toDF
//
//  println(time2-time1)
//  println(time3-time1)
//
//  wordDataFrame.show()
//
//  // 拆分训练集和测试集
//  val Array(trainData, testData) = wordDataFrame.randomSplit(Array(0.7, 0.3), seed = 11L)
//
//  val stopWords = Source.fromFile("D:\\WorkSpace\\Python_WorkSpace" +
//    "\\Python_classification\\dicts\\stop_words_CN").getLines().toArray
//
//  // token
//  var tokenizer = new Tokenizer()
//    .setInputCol("content")
//    .setOutputCol("words")
//
//  // 去除停用词
//  val stopWordsRemover = new StopWordsRemover()
//    .setStopWords(stopWords)
//    .setInputCol("words")
//    .setOutputCol("filtered")
//  //  val worddf = stopWordsRemover.transform(wordDataFrame)
//
//  // 构建向量空间模型
//  val cvModel = new CountVectorizer()
//    .setInputCol(stopWordsRemover.getOutputCol)
//    .setOutputCol("rawFeatures")
//
//  // 计算idf值，并根据向量空间模型中的tf值获得tfidf
//  val idfModel = new IDF()
//    .setInputCol(cvModel.getOutputCol)
//    .setOutputCol("features")
//  val output = new ObjectOutputStream(new FileOutputStream("D:/idfModel"))
//  output.writeObject(idfModel)
//
////  val inppput = new ObjectInputStream(new FileInputStream("D:/idfModel"))
////  val idfModel = inppput.readObject().asInstanceOf[IDF]
//
//  val featureSelector = new ChiSqSelector()
//    .setNumTopFeatures(500)
//    .setFeaturesCol(idfModel.getOutputCol)
//    .setLabelCol("label")
//    .setOutputCol("selectedFeatures")
//
//  val vectorSpacePipline = new Pipeline()
//    .setStages(Array(tokenizer, stopWordsRemover, cvModel, idfModel))
//  val vectorSpacePiplineM = vectorSpacePipline.fit(trainData)
//  val trainCM = vectorSpacePiplineM.transform(trainData)
//  val testCM = vectorSpacePiplineM.transform(testData)
//  trainCM.show()
//  testCM.show()

  // 转换数据类型
//  val train = trainCM.select("label", "features").map(row => {
//    LabeledPoint(row.getDouble(0), row.getAs[SparseVector](1))
//  })
//
//  val test = testCM.select("label", "features").map(row => {
//    LabeledPoint(row.getDouble(0), row.getAs[SparseVector](1))
//  })
//
//  // 朴素贝叶斯
//  val nBModel = NaiveBayes.train(train, lambda = 1.0, modelType = "multinomial")
//  nBModel.save(sc, "D:/nBModel")
//
//  val predictionAndLabels = test.map {case LabeledPoint(label, features) =>
//    val prediction = nBModel.predict(features)
//    (prediction, label)
//  }
//
//  //  训练模型SVM
////  val num = 200
////  val svmModel = SVMWithSGD.train(train, num)
////  val predictionAndLabels = test.map {case LabeledPoint(label, features) =>
////    val prediction = svmModel.predict(features)
////    (prediction, label)
////  }
//

  sc.stop()
}
