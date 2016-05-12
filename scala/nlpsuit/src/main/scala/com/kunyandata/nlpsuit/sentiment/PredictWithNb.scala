package com.kunyandata.nlpsuit.sentiment

import java.io._

import com.kunyandata.nlpsuit.util.TextPreprocessing
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{ChiSqSelectorModel, HashingTF, IDFModel}

/**
  * Created by zhangxin on 2016/3/28.
  * 基于情感分析模型的预测方法类
  */
object PredictWithNb{

  /**
    * 模型初始化
    * 初始化读取模型，给出模型路径，用从本地某路径读
    * @param path 模型路径
    * @return 模型Map[模型名称，模型]
    * @author zhangxin
    */
  def init(path: String): Map[String, Any] = {

    val fileList = new File(path)
    val modelList = fileList.listFiles()
    var modelMap = Map[String, Any]()

    modelList.foreach(cate => {

      val modelName=cate.getName
      val tempModelInput = new ObjectInputStream(new FileInputStream(cate))
      modelMap += (modelName -> tempModelInput.readObject())

    })

    modelMap
  }

  /**
    * 情感预测   _坤雁分词器
    * @param content 待预测文本
    * @param models  模型Map，由init初始化得到
    * @param stopWords 停用词
    * @param wordSegTyp 分词模式，0和1
    * @return 返回情感label编号
    * @author zhangxin
    */
  private def predict(content: String, models: Map[String, Any], stopWords: Array[String], wordSegTyp: Int): Double = {

    val wordSegNoStop = TextPreprocessing.process(content, stopWords,wordSegTyp)

    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))

    prediction
  }

  /**
    * 情感预测  _ansj分词器
    * @param content  待预测文章
    * @param models  模型Map[模型名称，模型]，由init初始化得到
    * @param stopWords 停用词
    * @return  返回情感label编号
    * @author zhangxin
    */
  private def predict(content: String, models: Map[String, Any], stopWords:Array[String]): Double = {

    val wordSegNoStop = TextPreprocessing.process(content, stopWords)

    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))

    prediction
  }

  /**
    * 情感预测   _坤雁分词器
    * @param content 文本内容
    * @param model 模型
    * @param stopWords 停用词表
    * @return 预测结果label: neg/neu/pos/neu_pos
    * @author zhangxin
    */
  def predictWithSigle(content: String, model: Map[String, Any], stopWords: Array[String], wordSegTyp: Int): String = {

    val temp = predict(content, model, stopWords, wordSegTyp: Int)

    val result=temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

  /**
    * 情感预测   _ansj分词
    * @param content 文本内容
    * @param model 模型
    * @param stopWords 停用词表
    * @return 预测结果label: neg/neu/pos/neu_pos
    * @author zhangxin
    */
  def predictWithSigle(content:String,model:Map[String, Any],stopWords:Array[String]): String ={

    val temp = predict(content,model, stopWords)

    val result=temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

  /**
    * 二级模型预测  _坤雁分词器
    * @param content  文章内容
    * @param arr  二级模型数组
    * @param stopWords  停用词表
    * @return 情感label
    * @author zhangxin
    */
  def predictWithFS(content:String,arr:Array[Map[String, Any]],stopWords:Array[String],wordSegTyp: Int): String ={

    var temp = predict(content, arr(0), stopWords, wordSegTyp: Int)

    if (temp == 4.0) temp = predict(content, arr(1), stopWords, wordSegTyp: Int)

    val result=temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

}
