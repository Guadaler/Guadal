package com.kunyandata.nlp.classification

/**
  * Created by QQ on 2016/4/1.
  */

import java.io._

import com.kunyandata.nlpsuit.feature.BetterChiSqSelector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TrainingProcess {

  /**
    * 基于RDD的训练过程，其中包括了序列化tf，idf，chisqselector，nbmodel 4个模型。
    *
    * @param train 训练集
    * @param test 测试集
    * @param parasDoc idf最小文档频数参数
    * @param parasFeatrues 特征选择数量参数
    * @return 返回（精度，召回率）
    */
  def trainingProcessWithRDD(train: RDD[(Double, Array[String])], test: RDD[(Double, Array[String])],
                             parasDoc: Array[Int], parasFeatrues: Array[Int], VSMlength: Int) = {

    // 构建hashingTF模型，同时将数据转化为LabeledPoint类型
    val hashingTFModel = new feature.HashingTF(VSMlength)
    val trainTFRDD = train.map(line => {
      val temp = hashingTFModel.transform(line._2)
      (line._1, temp)
    })

    // 计算idf
    val idfTemp = parasDoc.map(minDoc => {
      val idfModel = new feature.IDF(minDoc).fit(trainTFRDD.map(line => {line._2}))
      val labeledTrainTfIdf = trainTFRDD.map( line => {
        val temp = idfModel.transform(line._2)
        LabeledPoint(line._1, temp)
      })
      (minDoc, labeledTrainTfIdf, idfModel)
    })

    // 优化过的卡方检验选择器
    idfTemp.map(labeledTfidfTuple => {
      val betterChiSqTest = new BetterChiSqSelector()
      val ChiSqTestArray = betterChiSqTest.preFit(labeledTfidfTuple._2)
      val tempResult = parasFeatrues.map(numTopFeatrues => {
        val chiSqTestModel = betterChiSqTest.fit(ChiSqTestArray, numTopFeatrues)
        val selectedTrain = labeledTfidfTuple._2.map(line => {
          val temp = chiSqTestModel.transform(line.features)
          LabeledPoint(line.label, temp)
        })

        // 创建NB模型
        val nbModel = NaiveBayes.train(selectedTrain, 1.0, "multinomial")

        // 测试集同步
        val testRDD = test.map(line => {
          val temp = line._2
          val tempTf = hashingTFModel.transform(temp)
          val tempTfidf = labeledTfidfTuple._3.transform(tempTf)
          val tempSelected = chiSqTestModel.transform(tempTfidf)
          LabeledPoint(line._1, tempSelected)
        })

        // 预测
        val predictionAndLabels = testRDD.map {line =>
          val prediction = nbModel.predict(line.features)
          //      println((prediction, line.label))
          (prediction, line.label)
        }

        // 计算精度和召回率
        val metrics = new MulticlassMetrics(predictionAndLabels)
        (numTopFeatrues, (metrics.precision(1.0), metrics.recall(1.0)))
      }).toMap

      (labeledTfidfTuple._1, tempResult)
    }).toMap
  }

  /**
    * 网格参数寻优，并输出成文本
    *
    * @param df 输入的训练集，为交叉验证准备的5份训练测试数据
    * @param parasDoc idf最小文档频数参数的序列
    * @param parasFeatrues 特征选择数量参数的序列
    */
  def tuneParas(df: Array[Map[String, RDD[(Double, Array[String])]]], parasDoc:Array[Int], parasFeatrues:Array[Int], indusName: String) = {
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", "hdfs://222.73.57.12:9000")
    val fs = FileSystem.get(hdfsConf)
    val output = fs.create(new Path("/mlearning/ParasTuning/" + indusName))
    val writer = new PrintWriter(output)
    val parasTuningResult = df.map(data => {
      val VSMlength = countWords(data("train"))
      val temp = trainingProcessWithRDD(data("train"),
        data("test"), parasDoc, parasFeatrues, VSMlength)
      (VSMlength, temp)
    })

    // 将统计信息写入本地
    writer.write("行业\'" + indusName +"\'的统计信息如下:\n")
    parasDoc.foreach(paraDoc => {
      parasFeatrues.foreach(paraFea => {
        parasTuningResult.foreach(line => {
          val temp = line._2(paraDoc)(paraFea)
          writer.write( "语料库特征长度为" + line._1 + "\n")
          val outLabel = "minDoc和topFeatrueNum参数分别为: " + paraDoc + "," + paraFea
          writer.write(outLabel + "\t精度和召回率为: " + temp + "\n")
        })
        val preArray = parasTuningResult.map(_._2(paraDoc)(paraFea)._1)
        val recArray = parasTuningResult.map(_._2(paraDoc)(paraFea)._2)
        val avePrecision = preArray.sum / preArray.length
        val aveRecall = recArray.sum / recArray.length
        writer.write("参数组合:" + paraDoc + "," + paraFea +
          "的平均精度为: " + avePrecision + ",平均召回率为: " + aveRecall + "\n\n")
      })
    })
    writer.close()
  }

  /**
    * 从本地保存的文章对应行业文本数据变形为（行业，文章url数组）
    *
    * @param args 一个Array，每一个元素是一个tuple，包含文章url和对应的行业
    * @return 返回一个map，key为行业，value为url
    */
  private def getIndusTextUrl(args: Array[(String, String)]): Map[String, ArrayBuffer[String]] = {

    val result = mutable.Map[String, ArrayBuffer[String]]()
    args.foreach(line =>{
      val industries = line._2.split(",")
      industries.foreach(indus => {
        if (result.keys.toArray.contains(indus)){
          result(indus).append(line._1)
        }else{
          result.+=((indus, ArrayBuffer[String]()))
          result(indus).append(line._1)
        }
      })
    })
    result.toMap
  }

  /**
    * 匹配数据集合中对应的url的数据
    *
    * @param targetIDList 目标id（一般的，用文章的url作为文章的唯一标识）
    * @param dataSet 所有文章的数据集，每一个元素为（url, 正文）
    * @return 返回一个数组，元素是RDD[(Double, Array[String])]
    */
  private def matchRDD(targetIDList: Array[String], dataSet:RDD[(String, Array[String])]): RDD[(Double, Array[String])] ={

    //    val intersectId = idA.toSet & idB.toSet
    val poInst = dataSet.map(line => {
      if (targetIDList.contains(line._1)) (1.0, line._2)
    }).filter(_ != ()).map(_.asInstanceOf[(Double, Array[String])]).cache()
    val poInstNum = poInst.count()

    val neInst = dataSet.map(line => {
      if (!targetIDList.contains(line._1)) (0.0, line._2)
    }).filter(_ != ()).map(_.asInstanceOf[(Double, Array[String])]).cache()
    val neInstNum = neInst.count()

    if (poInstNum <= neInstNum){
      poInst.++(neInst.sample(withReplacement = false, fraction = poInstNum*1.0/neInstNum, seed = 2016))
    }else{
      poInst.++(neInst.sample(withReplacement = true, fraction = poInstNum*1.0/neInstNum, seed = 2016))
    }
  }

  /**
    * 计算语料库中的词汇数量
    *
    * @param trainingSet 语料库RDD
    * @return 返回一个整形
    */
  def countWords(trainingSet: RDD[(Double, Array[String])]): Int = {
    val wordCount = trainingSet.flatMap(training => {
      training._2
    }).collect().toSet.size
    wordCount
  }

  /**
    * 根据最优参数组合训练模型
    *
    * @param train 训练集
    * @param minDF 计算Idf的最小文档频数
    * @param topFeat 特征维度
    * @return 返回保存模型的Mpa和模型在训练集上的精度
    */
  private def trainModels(train: RDD[(Double, Array[String])], minDF: Int, topFeat: Int) = {
    // 计算tf
    val VSMlength = countWords(train)
    val hashingTFModel = new feature.HashingTF(VSMlength)
    val trainTFRDD = train.map(line => {
      val temp = hashingTFModel.transform(line._2)
      (line._1, temp)
    })
    // 计算idf
    val idfModel = new feature.IDF(minDF).fit(trainTFRDD.map(line => {line._2}))
    val labeedTrainTfIdf = trainTFRDD.map( line => {
      val temp = idfModel.transform(line._2)
      LabeledPoint(line._1, temp)
    })
    // 卡方降维
    val chiSqSelectorModel = new feature.ChiSqSelector(topFeat).fit(labeedTrainTfIdf)
    val selectedTrain = labeedTrainTfIdf.map(line => {
      val temp = chiSqSelectorModel.transform(line.features)
      LabeledPoint(line.label, temp)
    })
    // 贝叶斯分类器
    val nbModel = NaiveBayes.train(selectedTrain, 1.0, "multinomial")
    val models = Map("tfModel" -> hashingTFModel, "idfModel" -> idfModel,
      "chiSqSelectorModel" -> chiSqSelectorModel, "nbModel" -> nbModel)
    val predictionAndLabels = selectedTrain.map {line =>
      val prediction = nbModel.predict(line.features)
      //      println((prediction, line.label))
      (prediction, line.label)
    }
    // 计算精度和召回率
    val metrics = new MulticlassMetrics(predictionAndLabels)
    (models, metrics.precision(1.0))
  }

  /**
    * 根据参数，序列化训练模型到制定hdfs上
    *
    * @param defaultFS hdfs的地址
    * @param path hdfs中的路径
    * @param train 训练集
    * @param category 行业
    * @param minDF 计算idf值的最小文档频数
    * @param topFeat 特征数量
    */
  def outPutModels(defaultFS: String, path: String, train: RDD[(Double, Array[String])], category: String, minDF: Int, topFeat: Int) = {
    val models = trainModels(train, minDF, topFeat)
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", defaultFS)
    val fs = FileSystem.get(hdfsConf)
    val modelOutput = new ObjectOutputStream(fs.create(new Path(path+ category + ".models")))
    modelOutput.writeObject(models._1)
  }

  /**
    * 根据参数，输出训练模型到制定本地路径下
    *
    * @param path 模型保存路径
    * @param train 训练集
    * @param category 行业
    * @param minDF 计算idf值的最小文档频数
    * @param topFeat 特征数量
    */
  def outPutModels(path: String, train: RDD[(Double, Array[String])], category: String, minDF: Int, topFeat: Int): Unit = {
    val models = trainModels(train, minDF, topFeat)
    val modelOutput = new ObjectOutputStream(new FileOutputStream(path + category + ".models"))
    modelOutput.writeObject(models._1)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("outputModels_" + args(0))
//      .setMaster("local")
//      .setMaster("spark://222.73.57.12:7077")
//      .set("spark.local.ip","192.168.2.65")
//      .set("spark.driver.host","192.168.2.65")
//      .set("spark.executor.memory", "15G")
//      .set("spark.executor.cores", "4")
//      .set("spark.cores.max", "8")
    val sc = new SparkContext(conf)

    // 根据最优参数组合，训练并输出模型
    val parasSets = sc.textFile("hdfs://222.73.57.12:9000/mlearning/ParasSets").collect()
    parasSets.foreach(each => {
      val Array(indus, minDF, topFeat) = each.split("\t")
      if (args(0) == indus){
        val trainData = sc.textFile("hdfs://222.73.57.12:9000/mlearning/trainingData/trainingWithIndus/" + indus)
          .map(line => {
            val Array(label, content) = line.split("\t")
            (label.toDouble, content.split(","))
          })
        outPutModels("hdfs://222.73.57.12:9000", "/mlearning/Models/", trainData, indus, minDF.toInt, topFeat.toInt)
      }
    })
//    val parasSets = Source.fromFile("/home/mlearning/ParasSets_test").getLines().take(1)
//    parasSets.foreach(line => {
//      val Array(indus, minDf, topNum) = line.split("\t")
//      val trainData = sc.parallelize(Source.fromFile("/home/mlearning/trainingData/trainingWithIndus/" + indus)
//        .getLines().toSeq).map(row => {
//        val Array(label, content) = row.split("\t")
//        (label.toDouble, content.split(","))
//      })
//      outPutModels(trainData, indus, minDf.toInt, topNum.toInt, hdfs = false)
//    })





    //    val text = Source.fromFile("D:/mlearning/trainingLabel.new").getLines().toArray.map(line => {
    //      val temp = line.split("\t")
    //      (temp(0), temp(1))
    //    })
    //
    //    val writer = new PrintWriter(new File("D:/labeledContent"))
    //    val a = getIndusTextUrl(text)
    //    a.foreach(line => {
    //      writer.write(line._1 + "\t" + line._2.mkString(",") + "\n")
    //    })
    //    writer.close()

    // 获取每个行业的url集合
    //    val labeledContent = sc.textFile("hdfs://222.73.57.12:9000/mlearning/trainingData/labeledContent").collect().map(line => {
    //      val temp = line.split("\t")
    //      (temp(0), temp(1).split(","))
    //    }).toMap

    // #################################################################################################################
    // ##########################################    基于5阶交叉验证的参数网格寻优过程    ##################################
    // #################################################################################################################
//    val trainingData = sc.textFile("hdfs://222.73.57.12:9000/mlearning/trainingData/trainingWithIndus/" + args(0)).repartition(16).cache()
//    val tempRDD = trainingData.map(line => {
//      val temp = line.split("\t")
//      (temp(0).toDouble, temp(1).split(","))
//    }).randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
//    val dataSet = Array(
//      Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(2)).++(tempRDD(3)), "test" -> tempRDD(4)),
//      Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(2)).++(tempRDD(4)), "test" -> tempRDD(3)),
//      Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(2)),
//      Map("train" -> tempRDD(0).++(tempRDD(2)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(1)),
//      Map("train" -> tempRDD(1).++(tempRDD(2)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(0))
//    )
//    TrainingProcess.tuneParas(dataSet, args(1).split(",").map(_.toInt), args(2).split(",").map(_.toInt), args(0))
    // #################################################################################################################
    // #################################################################################################################
    // #################################################################################################################





    // #################################################################################################################
    // ########################################    根据行业分割数据集    #################################################
    // #################################################################################################################
    // 获取总训练集
    //    val totalTrianRDD = sc.textFile("hdfs://222.73.57.12:9000/mlearning/trainingData/segTrainSet").repartition(4).map(line => {
    //      val temp = line.split("\t")
    //      if (temp.length == 2) {
    //        (temp(0), temp(1).split(","))
    //      }
    //    }).filter(_ != ()).map(_.asInstanceOf[(String, Array[String])])

    //    // 根据行业分割数据集，并输出成本地文本
    //    val writer = new PrintWriter(new File("D:/mlearning/trainingSets/trainingStatic"))
    //    labeledContent.foreach(line => {
    //      val traingingWriter = new PrintWriter(new File("D:/mlearning/trainingSets/" + line._1))
    //      val temp = matchRDD(line._2, totalTrianRDD).collect()
    //      temp.foreach(content => {
    //        val outPutString = content._1 + "\t" + content._2.mkString(",") + "\n"
    //        traingingWriter.write(outPutString)
    //      })
    //      traingingWriter.close()
    //      val posTemp = temp.count(_._1 == 1.0)
    //      val negTemp = temp.count(_._1 == 0.0)
    //      writer.write("训练集\'" + line._1 + "\'的数量为" + ":" + temp.length + "\t"
    //        + "其中正例的样本数量为" + posTemp + "\t"
    //        + "其中负例的样本数量为" + negTemp + "\n")
    //      writer.flush()
    //    })
    //    writer.close()
    // #################################################################################################################
    // #################################################################################################################
    // #################################################################################################################
    sc.stop()
  }
}
