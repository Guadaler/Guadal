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
    * 预测方法
    *
    * @param content 正文字符串
    * @param models 模型Map，由intiModel方法提供
    * @return 返回一个字符串，包含了行业名称，例子：“银行,保险”
    */
  def industryPredict(content: String, models: Map[String, Map[String, Any]], stopWords: Array[String]):String = {
    val wordSegJson = WordSeg.splitWord(content, 1)
    val wordSeg = WordSeg.getWords(wordSegJson)

    // 去除停用词
    val wordSegNoStop = WordSeg.removeStopWords(wordSeg, stopWords)
    val classificationResult = models.keys.map(key => {
      val tfModel = models(key)("tfModel").asInstanceOf[HashingTF]
      val tf = tfModel.transform(wordSegNoStop)

      // idf
      val idfModel = models(key)("idfModel").asInstanceOf[IDFModel]
      val tfidf = idfModel.transform(tf)

      // chisqselector
      val chisqSelector = models(key)("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
      val selectedFeatures = chisqSelector.transform(tfidf)

      //
      val nbModel = models(key)("nbModel").asInstanceOf[NaiveBayesModel]
      val prediction = nbModel.predict(selectedFeatures)
      if (prediction == 1.0) key
    })
    classificationResult.mkString(",")
  }

  val kkk = initModel("D:/test")
  val stopWords = getStopWords("D:\\WorkSpace\\Spark_WorkSpace" +
    "\\ein\\text_classification\\1.1\\Spark_NLP_suit\\src\\main" +
    "\\resources\\dicts\\stop_words_CN")
  println(industryPredict("16日上午，由中国社会科学院经济学部、中国社会科学院智库工作协调办公室主办，城市发展与环境研究所承办的“中国社会科学院高端智库论坛”之“2016年经济形势座谈会——聚焦：新常态、新改革、新发展”在京举行。\n\n　　“下一步财税金融改革恐怕是重头戏。”中国社科院经济学部主任李扬昨日表示，本轮财税改革基本停滞不前，原来提出的一些目标有可能推后，所以财税改革是“十三五”很有挑战性的任务，而当前的情况又给改革增加了一些挑战。\n\n　　李扬在发言中透露，从去年到今年，党中央、国务院在经济上从建设智库角度布置了一系列任务，一共有130多项。他表示，下一步社科院有“5+1”个问题需要学部展开。\n\n　　其中，“1”是指建立中国特色的社会主义政治经济学。“我的很多同辈人讨论中国经济问题、中国经济理论问题的时候有一个共同感觉，我们吃亏在没有自己的理论体系。”李扬说，建立这个学说也一直是中国经济学人努力的方向。\n\n　　此外，“5”则包括人口问题、金融周期问题、供应侧经济结构调整、财税金融改革和TPP、自贸区问题等五个具体议题。\n\n　　在这些议题中，李扬认为财税金融改革可能是下一步的重头戏。他表示，财税改革是“十三五”很有挑战性的任务。“我们大致能够判断，从现在到未来的很长时间内，中国的财政赤字、中国政府债务会迅速地、巨额地增加。我们要讨论巨额的政府债务和巨额的政府赤字下如何促进经济平稳发展。”\n\n　　在谈到今年的宏观经济着力点时，数量经济与技术经济研究所副所长李雪松也将加大积极财政政策的力度放在了首位。他表示，加大积极财政政策可以用在四个方面：投资领域，加强以基础设施建设为主的公共产品投资；在生产领域，完善营改增，降低制造业增值税的税率；在收入领域，结合扶贫攻坚工程通过加强转移支付的方式，直接间接地加快中低收入者的收入增长；在消费领域，要改善消费环境，打通消费流通环节的一些薄弱环节。积极财政政策要着眼于算总账，并显著加大实施力度，提振市场信心。", kkk, stopWords))
}
