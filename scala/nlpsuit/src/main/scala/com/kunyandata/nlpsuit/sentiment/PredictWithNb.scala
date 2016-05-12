package com.kunyandata.nlpsuit.sentiment

import java.io._

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{ChiSqSelectorModel, HashingTF, IDFModel}

/**
  * Created by zhangxin on 2016/3/28.
  * 基于情感分析模型的预测方法类
  */
object PredictWithNb{

  /**
    * 模型初始化
    *
    * @param modelPath 模型路径，此路径下包含四个模型
    * @return 模型Map[模型名称，模型]
    * @author zhangxin
    */
  def init(modelPath: String): Map[String, Any] = {

    //获取模型路径列表
    val fileList = new File(modelPath)
    val modelList = fileList.listFiles()

    //读取模型
    val modelMap: Map[String, Any] = modelList.map(cate=>{

      //模型名称
      val modelName = cate.getName

      //模型
      val tempModelInput = new ObjectInputStream(new FileInputStream(cate))

      (modelName, tempModelInput.readObject())
    }).toMap

    modelMap
  }

  /**
    *情感预测   +坤雁分词器
    *
    * @param content 待预测文本
    * @param models  模型Map，由init初始化得到
    * @param stopWords 停用词
    * @param kunConf 坤雁分词器配置
    * @return 返回情感label编号
    * @author zhangxin
    */
  private def predict(content: String, models: Map[String, Any], stopWords: Array[String], kunConf: KunyanConf): Double = {

    //对文本[分词+去停]处理
    val wordSegNoStop = TextPreprocessing.process(content, stopWords, kunConf)

    //用模型对处理后的文本进行预测
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))

    prediction
  }

  /**
    * 情感预测  _ansj分词器
    *
    * @param content  待预测文章
    * @param models  模型Map[模型名称，模型]，由init初始化得到
    * @param stopWords 停用词
    * @return  返回情感label编号
    * @author zhangxin
    */
  private def predict(content: String, models: Map[String, Any], stopWords: Array[String]): Double = {

    //对文本[分词+去停]处理
    val wordSegNoStop = TextPreprocessing.process(content, stopWords)

    //用模型对处理后的文本进行预测
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))

    prediction
  }

  /**
    * 情感预测   +坤雁分词器
    *
    * @param content 文本内容
    * @param model 模型
    * @param stopWords 停用词表
    * @return 预测结果label: neg/neu/pos/neu_pos
    * @author zhangxin
    */
  def predictWithSigle(content: String, model: Map[String, Any], stopWords: Array[String], kunConf: KunyanConf): String = {

    //获取预测结果编号
    val temp = predict(content, model, stopWords, kunConf)

    //将编号转成label，作为结果
    val result=temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

  /**
    * 情感预测   +ansj分词
    *
    * @param content 文本内容
    * @param model 模型
    * @param stopWords 停用词表
    * @return 预测结果label: neg/neu/pos/neu_pos
    * @author zhangxin
    */
  def predictWithSigle(content: String, model: Map[String, Any], stopWords: Array[String]): String = {

    //获取预测结果编号
    val temp = predict(content, model, stopWords)

    //将编号转成label，作为结果
    val result=temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }

  /**
    * 二级模型预测  +坤雁分词器
    *
    * @param content  文章内容
    * @param arr  二级模型数组
    * @param stopWords  停用词表
    * @return 情感label
    * @author zhangxin
    */
  def predictWithFS(content: String, arr: Array[Map[String, Any]], stopWords: Array[String], kunConf: KunyanConf): String = {

    //先用第一层模型进行第一次预测：neg 或者 其他
    var temp = predict(content, arr(0), stopWords, kunConf)

    //若判断为其他，则用第二层进行二次预测：neu 或者 pos
    if (temp == 4.0) {
      temp = predict(content, arr(1), stopWords, kunConf)
    }

    val result=temp match {
      case 1.0 => "neg"
      case 2.0 => "neu"
      case 3.0 => "pos"
      case 4.0 => "neu_pos"
    }

    result
  }
}
