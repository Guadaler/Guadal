/**
  * Created by QQ on 2016/4/7.
  */

import java.io._

import com.kunyandata.nlpsuit.feature.BetterChiSqSelector
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object BetterChiTest {
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
    val idfTemp = parasDoc.map(minDoc => { // 对参数最小文档频数进行循环，得到
      val idfModel = new feature.IDF(minDoc).fit(trainTFRDD.map(line => {line._2}))
      val labeledTrainTfIdf = trainTFRDD.map( line => {
        val temp = idfModel.transform(line._2)
        LabeledPoint(line._1, temp)
      })
      (minDoc, labeledTrainTfIdf, idfModel)
    })

    // 优化过的卡方检验选择器
    val writer = new PrintWriter(new File("D:/mlearning/工程建筑_par_" + VSMlength))
    val aaa = idfTemp.map(labeledTfidfTuple => {
      val betterChiSqTest = new BetterChiSqSelector()
      val ChiSqTestArray = betterChiSqTest.preFit(labeledTfidfTuple._2)
      val tempResult = parasFeatrues.map(numTopFeatrues => {

        // test start ##################################################################################################
        val chi = new ChiSqSelector(numTopFeatrues).fit(labeledTfidfTuple._2)
        val selected = labeledTfidfTuple._2.map(line => {
          val temp = chi.transform(line.features)
          LabeledPoint(line.label, temp)
        })
        val nb = NaiveBayes.train(selected, 1.0, "multinomial")
        val testrdd = test.map(line => {
          val temp = line._2
          val tempTf = hashingTFModel.transform(temp)
          val tempTfidf = labeledTfidfTuple._3.transform(tempTf)
          val tempSelected = chi.transform(tempTfidf)
          LabeledPoint(line._1, tempSelected)
        })
        val pal = testrdd.map {line =>
          val prediction = nb.predict(line.features)
          //      println((prediction, line.label))
          (prediction, line.label)
        }
        val met = new MulticlassMetrics(pal)


        // test end ####################################################################################################


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
        writer.write(labeledTfidfTuple._1 + "_" + numTopFeatrues + "\t"
          + "old: " + (met.precision(1.0), met.recall(1.0)) + "\n")
        writer.write(labeledTfidfTuple._1 + "_" + numTopFeatrues + "\t"
          + "new: " + (metrics.precision(1.0), metrics.recall(1.0)) + "\n")

        (numTopFeatrues, (metrics.precision(1.0), metrics.recall(1.0)))
      })

      (labeledTfidfTuple._1, tempResult)
    })
    writer.close()
    aaa
  }

  /**
    * 网格参数寻优，并输出成文本
    *
    * @param df 输入的训练集，为交叉验证准备的5份训练测试数据
    * @param parasDoc idf最小文档频数参数的序列
    * @param parasFeatrues 特征选择数量参数的序列
    */
  def tuneParas(df: Array[Map[String, RDD[(Double, Array[String])]]], parasDoc:Array[Int], parasFeatrues:Array[Int], indusName: String) = {
//    val hdfsConf = new Configuration()
//    hdfsConf.set("fs.defaultFS", "hdfs://222.73.34.92:9000")
//    val fs = FileSystem.get(hdfsConf)
//    val output = fs.create(new Path("/mlearning/ParasTuning/" + indusName))
//    val writer = new PrintWriter(output)
    val writer = new PrintWriter(new File("D:/mlearning/工程建筑" ))
    df.foreach(data => {
      val vocabNum = countWords(data("train"))
      writer.write("行业\'" + indusName + "\'的语料库特征长度为" + vocabNum + "\n")
      writer.flush()
      val results = trainingProcessWithRDD(data("train"),
        data("test"), parasDoc, parasFeatrues, vocabNum)
      results.foreach(lineDoc => {
        lineDoc._2.foreach(lineFeature => {
          val outLabel = "minDoc和topFeatureNum参数分别为: " + lineDoc._1 + "_" + lineFeature._1
          writer.write(outLabel + " 精度和召回率为: " + lineFeature._2 + "\n")
          writer.flush()
        })
        writer.write("\n")
        writer.flush()
      })
      //      val avePrecision = Statistic.sum(tempPandR.map(_._1).toArray) / tempPandR.count(_ != null)
      //      val aveRecall = Statistic.sum(tempPandR.map(_._2).toArray) / tempPandR.count(_ != null)
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
    }).filter(_ != ()).map(_.asInstanceOf[(Double, Array[String])])
    val poInstNum = poInst.count()

    val neInst = dataSet.map(line => {
      if (!targetIDList.contains(line._1)) (0.0, line._2)
    }).filter(_ != ()).map(_.asInstanceOf[(Double, Array[String])])
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

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MlTraining_").setMaster("local")
    val sc = new SparkContext(conf)
    val parasDoc = Array(1, 50, 100)
    val parasFeature = Array(500, 1000, 2000, 3000, 5000)
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
    //    val labeledContent = sc.textFile("hdfs://222.73.34.92:9000/mlearning/trainingData/labeledContent").collect().map(line => {
    //      val temp = line.split("\t")
    //      (temp(0), temp(1).split(","))
    //    }).toMap

    val trainingData = sc.parallelize(Source.fromFile("D:/mlearning/trainingData/trainingWithIndus/银行类").getLines().toSeq)
    val tempRDD = trainingData.map(line => {
      val temp = line.split("\t")
      (temp(0).toDouble, temp(1).split(","))
    }).randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
    val dataSet = Array(
      Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(2)).++(tempRDD(3)), "test" -> tempRDD(4)),
      Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(2)).++(tempRDD(4)), "test" -> tempRDD(3)),
      Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(2)),
      Map("train" -> tempRDD(0).++(tempRDD(2)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(1)),
      Map("train" -> tempRDD(1).++(tempRDD(2)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(0))
    )
    tuneParas(dataSet, parasDoc, parasFeature, "银行类")
  }
}
