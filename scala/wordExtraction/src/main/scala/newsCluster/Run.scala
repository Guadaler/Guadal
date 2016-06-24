package LDA

import java.io.{File, PrintWriter}

import com.kunyandata.nlpsuit.util.KunyanConf
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.{SparkConf, SparkContext}
import sentiment.Util.ParseJson

/**
  * Created by zhangxin on 2016/5/17.
  * LDA主程序 包含对流数据处理的全流程思路
  * 1.模型加载并转成online模式
  * 2.新文档集加载
  * 3.新文档集预处理
  * 4.预测新文档主题分布 使用LocalLDAMOdel提供的
  * 5.[部分就文档+新文档]合并作为训练集用em模式重新训练
  * 6.更新模型 和 词表
  */
object Run {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LDA").setMaster("local")
    val sc = new SparkContext(conf)
    LoggerUtil.warn("SC初始化结束》》》》》》》》》》》》》》")

    val modelPath = args(0) //"/home/zhangxin/LDARun/Model"
    val wordDictPath = args(1) // "/home/zhangxin/LDARun/Dict/vocab.txt"
    val stopWordsPath = args(2)  //"/home/zhangxin/LDA/Run/stop_words_CN"
    LoggerUtil.warn(modelPath)
    LoggerUtil.warn(wordDictPath)
    LoggerUtil.warn(stopWordsPath)

    //加载模型
    val models1 = DistributedLDAModel.load(sc,modelPath).toLocal

    //加载新数据
//    val newDataRDD = sc.wholeTextFiles(args(3))  //"/home/zhangxin/LDA/Run/Run/TestData/neg")
//    val newDataArray2 = newDataRDD.map(_._2).collect()
    val files=new File(args(3)).listFiles()
    LoggerUtil.warn(args(3)+" : "+files.size)
    val newDataArray=files.map(file =>sc.textFile(file.getAbsolutePath).toString())
    LoggerUtil.warn("新数据读取结束》》》》》》》》》》》》》》")

    //坤雁分词器配置项
    val configInfo = new ParseJson(args(4))  // "/home/zhangxin/LDA/Run/config.json")
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    //新数据预处理
    val vocab = sc.textFile(wordDictPath).collect  //原始词表
    val (old_cvModel, newDataVetors) = Pretreat.doc2VectorsPredict(sc, newDataArray, stopWordsPath, kunyanConfig, vocab)
    LoggerUtil.warn("新数据预处理结束》》》》》》》》》》》》》》")

    //预测并结果输出
    val result = models1.topicDistributions(newDataVetors)
    LoggerUtil.warn("预测结束》》》》》》》》》》》》》》")
    result.collect().foreach(line => LoggerUtil.info(line._1 + " :  " + line._2))

    //重新训练 并更新模型
    val modelArgs=Array(
      9,    //K
      200,  //maxIteration
      5,    //DocConcentration(5)
      5,    //TopicConcentration(5)
      0     //Seed(0L)

    )

    //数据合并+预处理
    val oldData = sc.wholeTextFiles("")
    val trainData = oldData.map(_._2).collect().toBuffer ++ newDataArray
    val vocabsize = 10000
    val (cvModel, trainDataVetors) = Pretreat.doc2Vectors(sc, trainData.toArray, stopWordsPath, kunyanConfig, vocabsize)

    //重新训练
    val newLDAModel = Train.trainWithEM(sc, modelArgs, trainDataVetors)
    LoggerUtil.warn("重新训练结束》》》》》》》》》》》》》》")

    //更新模型
    UtilLDA.deleteModel(modelPath)
    newLDAModel.save(sc, modelPath)
    LoggerUtil.warn("模型更新结束》》》》》》》》》》》》》》")

    //更新词表
    val newVocab = cvModel.vocabulary
    val wr = new PrintWriter(wordDictPath, "utf-8")
    newVocab.foreach(word => wr.append(word).flush())
    LoggerUtil.warn("词表更新结束》》》》》》》》》》》》》》")

    //计算模型的平均相似度作为模型的好坏的评价指标
    LoggerUtil.warn("模型的平均相似度为： "+UtilLDA.calculateAverageSimilar(newLDAModel,modelArgs(0)))

  }
}
