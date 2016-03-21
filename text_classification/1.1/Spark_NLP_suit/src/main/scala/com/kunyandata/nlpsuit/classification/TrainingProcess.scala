package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.SparseVector
import com.kunyandata.nlpsuit.util.WordSeg
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object TrainingProcess{

  /**
    * 基于RDD的训练过程，其中包括了序列化tf，idf，chisqselector，nbmodel 4个模型。
 *
    * @param train 训练集
    * @param test 测试集
    * @param parasDoc idf最小文档频数参数
    * @param parasFeatrues 特征选择数量参数
    * @return 返回（精度，召回率）
    */
  def trainingProcessWithRDD(train: RDD[Seq[Object]], test: RDD[Seq[Object]], parasDoc: Int, parasFeatrues: Int) = {
    // 构建hashingTF模型，同时将数据转化为LabeledPoint类型
    val hashingTFModel = new feature.HashingTF(55000)
    val trainTFRDD = train.map(line => {
      val temp = hashingTFModel.transform(line(2).asInstanceOf[Seq[String]])
      (if(line(1).asInstanceOf[String] == "881155") 1.0 else 0.0, temp)
    })

    val tfModelOutput = new ObjectOutputStream(new FileOutputStream("D:/tfModel"))
    tfModelOutput.writeObject(hashingTFModel)

    // 计算idf
    val idfModel = new feature.IDF(parasDoc).fit(trainTFRDD.map(line => {line._2}))
    val labeedTrainTfIdf = trainTFRDD.map( line => {
      val temp = idfModel.transform(line._2)
      LabeledPoint(line._1, temp)
    })

    val idfModelOutput = new ObjectOutputStream(new FileOutputStream("D:/idfModel"))
    idfModelOutput.writeObject(idfModel)

    // 卡方降维特征选择器
    val chiSqSelectorModel = new feature.ChiSqSelector(parasFeatrues).fit(labeedTrainTfIdf)
    val selectedTrain = labeedTrainTfIdf.map(line => {
      val temp = chiSqSelectorModel.transform(line.features)
      LabeledPoint(line.label, temp)
    })

    val chiSqSelectorModelOutput = new ObjectOutputStream(new FileOutputStream("D:/chiSqSelectorModel"))
    chiSqSelectorModelOutput.writeObject(chiSqSelectorModel)

    println("+++++++++++++++++++++++++++++++++++++++++++++特征选择结束++++++++++++++++++++++++++++++++++++++++++++++++++")

    // 创建贝叶斯分类器
    val nbModel = NaiveBayes.train(selectedTrain, 1.0, "multinomial")

    val nbModelOutput = new ObjectOutputStream(new FileOutputStream("D:/nbModel"))
    nbModelOutput.writeObject(nbModel)

    // 根据训练集同步测试集特征
    val testRDD = test.map(line => {
      val temp = line(2).asInstanceOf[Seq[String]]
      val tempTf = hashingTFModel.transform(temp)
      val tempTfidf = idfModel.transform(tempTf)
      val tempSelected = chiSqSelectorModel.transform(tempTfidf)
      LabeledPoint(if(line(1).asInstanceOf[String] == "881155") 1.0 else 0.0, tempSelected)
    })

    val predictionAndLabels = testRDD.map {line =>
      val prediction = nbModel.predict(line.features)
//      println((prediction, line.label))
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
    (metrics.precision(1.0), metrics.recall(1.0))
  }

  /**
    * 基于dataframe的训练，主要用于网格参数寻优。
 *
    * @param sc sparkcontext
    * @param train 训练集
    * @param test 测试集
    * @param parasDoc idf最小文档频数参数
    * @param parasFeatrues 特征选择数量参数
    * @return 返回（精度，召回率）
    */
  def trainingProcessWithDF(sc: SparkContext, train:RDD[Seq[Object]], test: RDD[Seq[Object]], parasDoc: Int, parasFeatrues: Int) = {
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

//     构建向量空间模型
    val hashingTFModel = new HashingTF()
      .setInputCol(stopWordsRemover.getOutputCol)
      .setOutputCol("rawFeatures")
      .setNumFeatures(55000)

//    val cvModel = new CountVectorizer()
//      .setInputCol(stopWordsRemover.getOutputCol)
//      .setOutputCol("rawFeatures")

    // 计算idf值，并根据向量空间模型中的tf值获得tfidf
    val idfModel = new IDF()
      .setInputCol(hashingTFModel.getOutputCol)
      .setOutputCol("features")
      .setMinDocFreq(parasDoc)

    //  val inppput = new ObjectInputStream(new FileInputStream("D:/idfModel"))
    //  val idfModel = inppput.readObject().asInstanceOf[IDF]

    val featureSelector = new ChiSqSelector()
      .setNumTopFeatures(parasFeatrues)
      .setFeaturesCol(idfModel.getOutputCol)
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val vectorSpacePipline = new Pipeline()
      .setStages(Array(stopWordsRemover, hashingTFModel, idfModel, featureSelector))
    val vectorSpacePiplineM = vectorSpacePipline.fit(dataDF)
    val trainCM = vectorSpacePiplineM.transform(dataDF)
    val testCM = vectorSpacePiplineM.transform(testDF)
    trainCM.show


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
    (metrics.precision(1.0), metrics.recall(1.0))
  }

  /**
    * 网格参数寻优，并输出成文本
 *
    * @param df 输入的训练集，为交叉验证准备的5份训练测试数据
    * @param parasDoc idf最小文档频数参数的序列
    * @param parasFeatrues 特征选择数量参数的序列
    */
  def tuneParas(sc: SparkContext, df: Seq[Map[String, RDD[Seq[Object]]]], parasDoc:Array[Int], parasFeatrues:Array[Int]) = {
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", "hdfs://222.73.34.92:9000")
    val fs = FileSystem.get(hdfsConf)
    val output = fs.create(new Path("/mlearning/trainingResult"))
    val writer = new PrintWriter(output)
    var result:Map[String,Tuple2[Double, Double]] = Map()
    parasDoc.foreach(paraDoc => {
      parasFeatrues.foreach(paraFeatrues => {
        df.foreach(data => {
          val paraSets = paraDoc.toString + "_" + paraFeatrues.toString
          val results = trainingProcessWithDF(sc, data("train"), data("test"), paraDoc, paraFeatrues)
          result += (paraSets -> results)
          val writeOut = paraSets + "\t\tPrecision:" + results._1 + "\tRecall:" + results._2 + "\n"
          writer.write(writeOut)
        })
        writer.write("\n")
      })
      writer.write("\n\n")
    })
    result.foreach(println)
    writer.close()
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

  val dataSets = dataRDD.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
  val dataSet = Seq(
    Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)),
    Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(4)), "test" -> dataSets(3)),
    Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(2)),
    Map("train" -> dataSets(0).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(1)),
    Map("train" -> dataSets(1).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(0))
  )
//  tuneParas(dataSet, Array(1,2),
//    Array(500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000,
//      5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000))

//  val result1 = trainingProcessWithDF(sc, dataSet(0)("train"), dataSet(0)("test"), 0, 500)
//  val result2 = trainingProcessWithRDD(dataSet(0)("train"), dataSet(0)("test"), 0, 500)
//  println(result1)
//  println(result2)
//  trainingProcessWithDF(sc, dataSet(0)("train"), dataSet(0)("test"), 2, 500)
//  trainingProcessWithRDD(trainDataRDD, testDataRDD)

  sc.stop()
}
