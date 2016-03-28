package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import java.io._
import java.util.Date

import com.kunyandata.nlpsuit.util.TextProcess
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.classification.NaiveBayesModel

import scala.io.Source


object Bayes {

  def main(args: Array[String]) {
    val segAppPath = "/home/mlearning/bin/"
    val conf = new SparkConf().setAppName("mltest").setMaster("local")
    val sc = new SparkContext(conf)
    val modelMap = initModel("hdfs://222.73.34.92:9000/mlearning/testModel")
    val stopWords = getStopWords("hdfs://222.73.34.92:9000/mlearning/dicts/stop_words_CN")
    val stopWordsBr = sc.broadcast(stopWords)
    val content = "本期高频数据显示：1月信贷扩张剧烈达历年之最，同时受人民币外汇市场波动与疲弱出口影响，宽口径外汇占款下降显著，2月上旬央行货币净投放更创历史新高。\n　　信贷极度宽松背景下利率市场表现稳定，只有信托类产品收益率在2月上旬显著下降。信贷扩张还未传导至实体经济，其中工业方面钢铁行业去产能进一步加速并为价格托底，煤炭、水泥价格依旧徘徊谷底；航运价格仍持续走低；受春节假期影响新房、二手房与土地交易量均表现惨淡。\n　　2月5日当周央行公开市场货币净投放为3300亿元，尽管比上期6900亿有所下降，但仍保持历史高位。2月上旬银行间市场隔夜回购利率上升29个基点至2.26%，7天回购利率下降4个基点至3.06%。2月上旬国债收益率基本维持不变，AAA级与AA级企业债收益率分别上升2个基点。AA+级收益率小幅下降。\n　　钢铁的消费中，基建和房地产超过50%，机械和汽车接近25%，所以钢铁的供求和价格数据是观测固定资产投资和消费的重要指标。钢铁去产能步伐加速，价格回落。1月下旬粗钢产量同比增长-10.81%，连续5旬出现两位数负增长。2月上旬唐山钢坯价格维持在1540元/吨，为近一个月来新低。1月出口（除中国香港地区外）同比增长-12.33%，是近10个月来最大负增长。\n　　1月份新增社会融资总额34173亿元，环比猛增88%，增长主要来源于新增人民币贷款项（同比增长72%，达到25370亿元）。\n　　1月金融机构新增外汇占款减少23766亿元，达到11年来的最大当月减少值。1月中国百城新建住宅价格上升0.42%，其中一、二、三线城市环比分别上升1.89%、0.80%、-1.08%。2014年房地产投资占GDP14.9%，占固定资产投资的18.6%，房地产贷款余额占总贷款余额比21.27%，土地出让金占政府收入30.4%。"
    val cateString = industryPredict(content, modelMap, segAppPath, stopWordsBr)
    println(cateString)
  }


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
  def industryPredict(content: String, models: Map[String, Map[String, Any]], segAppPath: String, stopWordsBr: Broadcast[Array[String]]): String = {
    val wordSegNoStop = TextProcess.process(content, segAppPath, stopWordsBr)
    val classificationResult = models.keys.map(key => {
      val time1 = new Date().getTime
      val prediction = models(key)("nbModel").asInstanceOf[NaiveBayesModel]
        .predict(models(key)("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
          .transform(models(key)("idfModel").asInstanceOf[IDFModel]
            .transform(models(key)("tfModel").asInstanceOf[HashingTF]
              .transform(wordSegNoStop))))
      val time2 = new Date().getTime
      if (prediction == 1.0) key
    })
    classificationResult.mkString(",")
  }
}
