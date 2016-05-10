package classification

import java.io.{FileOutputStream, ObjectOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by zx on 2016/3/28.
  */
object TrainWithNb {

  /**
    * 基于RDD的贝叶斯训练
    * 备注：仅训练测试，模型不保存
 *
    * @param sc
    * @param filepath  数据集文件路径(经过预处理的数据集)
    */
  def nbTrain(sc:SparkContext,filepath:String): Unit ={
    val trainData=sc.textFile(filepath)

    //基于RDD的训练流程
    val dataRDD=trainData.map(line =>{
      val temp = line.split("#")
      (temp(0).toDouble, temp(1).split(","))
    })

    println("数据载入结束")
    //按照8:2的比例随机划分数据集
    val dataSets = dataRDD.randomSplit(Array(0.8, 0.2), seed = 2016L)
    val train=dataSets(0).cache()
    val test=dataSets(1).cache()

    val result = TrainingProcess.trainingProcessWithRDD(train,test, Array(0), Array(1500),10000)
//    val result = TrainingProcess.trainModels(dataRDD,0,1000)
    println(result)
    sc.stop()
  }

  /**
    * 基于RDD的贝叶斯训练,并保存模型到默认的hdfs "hdfs://222.73.57.12:9000"
 *
    * @param sc SparkContext
    * @param filepath 数据集路径
    * @param indus  模型名称（行业名称）
    * @param minDF 最小文档阈值 默认为0
    * @param topFeat 最大特征空间值
    */
  def nbTrainToHdfs(sc:SparkContext,filepath:String,indus: String,minDF:Int,topFeat:Int): Unit ={
    val hdfsAddress="hdfs://222.73.57.12:9000"
    val trainData=sc.textFile(filepath)
    //基于RDD的训练流程
    val dataRDD=trainData.map(line =>{
      val temp = line.split("#")
      (temp(0).toDouble, temp(1).split(","))
    })
    val result = TrainingProcess.outPutModels(hdfsAddress,filepath,dataRDD,indus,minDF,topFeat)
    println(result)
    sc.stop()
  }

  /**
    * 基于RDD的贝叶斯训练，并保存模型到本地路径
    * 备注：数据不划分，全部用于train
 *
    * @param sc
    * @param filepath 数据集路径
    * @param outPath 模型输出路径 如E:/svmmodels/
    * @param indus  模型名称（行业名称）
    * @param minDF 最小文档阈值
    * @param topFeat 最大特征空间值
    */
  def nbTrainToLocal(sc:SparkContext,filepath:String,outPath:String,indus: String,minDF:Int,topFeat:Int): Unit ={
    val trainData=sc.textFile(filepath)
    //基于RDD的训练流程
    val dataRDD=trainData.map(line =>{
      val temp = line.split("#")
      (temp(0).toDouble, temp(1).split(","))
    })
//    val result=TrainingProcess.outPutModels(outPath,dataRDD,indus,minDF,topFeat)
    val result=TrainWithNb.trainModels_zx(dataRDD,outPath,minDF,topFeat)
//    println(result)
    sc.stop()
  }


  /**
    * 根据最优参数组合训练模型
    *
    * @param train 训练集
    * @param minDF 计算Idf的最小文档频数
    * @param topFeat 特征维度
    * @return 返回保存模型的Mpa和模型在训练集上的精度
    */
  private def trainModels_zx(train: RDD[(Double, Array[String])],outPath:String, minDF: Int, topFeat: Int) = {
    // 计算tf
    //    val VSMlength = countWords(train)
    val VSMlength=10000
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

    //模型写入
    val modelout=outPath+"\\Feature_"+topFeat
    val tfModelOutput = new ObjectOutputStream(new FileOutputStream(modelout+"/tfModel"))
    tfModelOutput.writeObject(hashingTFModel)
    val idfModelOutput = new ObjectOutputStream(new FileOutputStream(modelout+"/idfModel"))
    idfModelOutput.writeObject(idfModel)
    val chiSqSelectorModelOutput = new ObjectOutputStream(new FileOutputStream(modelout+"/chiSqSelectorModel"))
    chiSqSelectorModelOutput.writeObject(chiSqSelectorModel)
    val nbModelOutput = new ObjectOutputStream(new FileOutputStream(modelout+"/nbModel"))
    nbModelOutput.writeObject(nbModel)

    //预测
    val predictionAndLabels = selectedTrain.map {line =>
      val prediction = nbModel.predict(line.features)
      //      println((prediction, line.label))
      (prediction, line.label)
    }
    // 计算精度和召回率
    val metrics = new MulticlassMetrics(predictionAndLabels)
    println("Neg精度为："+metrics.precision(1.0)+"   召回率为："+metrics.recall(1.0))
    println("Neu和Pos精度为："+metrics.precision(4.0)+"   召回率为："+metrics.recall(4.0))
    (models, metrics.precision(1.0))
  }


  /**
    * 基于网格参数寻优的训练
 *
    * @param sc
    * @param filepath  数据集路径
    */
  def tuneParasTrain(sc:SparkContext,filepath:String): Unit ={
    val trainData=sc.textFile(filepath)
    val dataRDD=trainData.map(line=>{
      val temp=line.split("#")
      (temp(0).toDouble, temp(1).split(","))  //K_V二元组
    })

    //数据分割
    val dataSets = dataRDD.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
    val dataSet = Array(
//      Map[Array[String],Array[String]](dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)
      Map("train" ->dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(4)), "test" -> dataSets(3)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(2)),
      Map("train" -> dataSets(0).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(1)),
      Map("train" -> dataSets(1).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(0))
    )

    TrainingProcess.tuneParas(dataSet,Array(1),Array(500),"Testzx")
  }
}
