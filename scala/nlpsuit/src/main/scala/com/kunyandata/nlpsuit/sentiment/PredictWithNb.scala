package com.kunyandata.nlpsuit.sentiment

import java.io._

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{ChiSqSelectorModel, HashingTF, IDFModel}
import scala.io.Source

/**
  * Created by zx on 2016/3/28.
  * 基于机器学习的情感分析
  */
object PredictWithNb extends App{


  /**
    * 初始化读取模型，给出模型路径，用从本地某路径读
 *
    * @param path 模型路径
    * @param methodNum 为了与从hdfs读取区分，加入方法编号作标记
    * @return  模型Map[模型名称，模型]
    */
  def init(path: String,methodNum:Int): Map[String, Any] = {
    val fileList = new File(path)
    val modelList = fileList.listFiles()
    var modelMap:Map[String, Any] = Map()
    modelList.foreach(cate => {
      val modelName=cate.getName
      val tempModelInput = new ObjectInputStream(new FileInputStream(cate)).readObject()
      modelMap += (modelName -> tempModelInput)
    })
    modelMap
  }

  /**
    * 初始化模型
    *
    * @param path 模型本地保存路径
    * @return 模型map
    */
  def init(path: String): Map[String, Any] = {

    val fileList = new File(path)
    val modelList = fileList.listFiles()
    var modelMap:Map[String, Any] = Map()
    modelList.foreach(cate => {
      val modelName=cate.getName
      val tempModelInput = new ObjectInputStream(new FileInputStream(cate))
      modelMap += (modelName -> tempModelInput.readObject())
    })

    modelMap

  }

  /**
    * 情感预测   _坤雁分词
    * @param content  待预测文章
    * @param models  模型Map[模型名称，模型]，由init初始化得到
    * @param stopWordsArr 停用词
    * @param kunyanConf 设置坤雁分词器
    * @return  返回情感label编号
    */
  def predict(content: String, models: Map[String, Any], stopWordsArr: Array[String],
              kunyanConf: KunyanConf): Double = {
    val wordSegNoStop = TextPreprocessing.process(content, stopWordsArr, kunyanConf)
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
    * @param stopWordsArr 停用词
    * @return  返回情感label编号
    */
  def predict(content: String, models: Map[String, Any], stopWordsArr:Array[String]): Double = {

    val wordSegNoStop = TextPreprocessing.process(content, stopWordsArr)
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))

    prediction

  }

  /**
    * 单模型+单篇文章 +坤雁分词
 *
    * @param content 文章内容
    * @param model 模型
    * @param stopWordsArr 停用词
    * @return  情感label
    * @author zhangxin
    */
  def predictWithSigle(content: String, model: Map[String, Any],
                       stopWordsArr: Array[String], kunyanConf: KunyanConf): String ={

    val temp = predict(content,model, stopWordsArr,kunyanConf)
    val result=replaceLabel(temp)

    result

  }
  /**
    * 用ansj分词
    */
  def predictWithSigle(content:String,model:Map[String, Any],stopWordsArr:Array[String]): String ={

    val temp = predict(content,model, stopWordsArr)
    val result=replaceLabel(temp)

    result

  }

  /**
    * 二级模型 +单篇文章
 *
    * @param content  文章内容
    * @param arr  二级模型数组
    * @param stopWordsArr  停用词表
    * @return 情感label
    * @author zhangxin
    */
  def predictWithFS(content:String,arr:Array[Map[String, Any]],stopWordsArr:Array[String],kunyanConf: KunyanConf): String ={

    var temp = predict(content,arr(0), stopWordsArr,kunyanConf: KunyanConf)

    if (temp == 4.0)
      temp = predict(content,arr(1), stopWordsArr,kunyanConf: KunyanConf)

    replaceLabel(temp)

  }

  /**
    * 二级模型，批量预测
 *
    * @param filepath  批量预测文章路径
    * @param outpath  输出预测结果
    * @param arr  二级模型数组
    * @param stopWordsArr  停用词表
    * @author zhangxin
    */
  def predictManyWithFS(filepath:String, outpath: String, arr: Array[Map[String, Any]],
                        stopWordsArr: Array[String], kunyanConf: KunyanConf): Unit ={

    val wr = new PrintWriter(outpath,"utf-8")
    val files = new File(filepath).listFiles()
    for(file <- files) {

      val title = file.getName.substring(0, file.getName.indexOf(".txt"))
      var contentstr = ""

      for (line <- Source.fromFile(file).getLines()) {
        contentstr += line
      }

      var temp = predict(contentstr,arr(0), stopWordsArr,kunyanConf: KunyanConf)

      if (temp == 4.0)
        temp = predict(contentstr,arr(1), stopWordsArr,kunyanConf: KunyanConf)

      val result=replaceLabel(temp)
      wr.write("【" + result + "】" + title + "\n")
      wr.flush()
    }
  }

  /**
    * 编号替换成标签  如 1.0 =》 neg
 *
    * @param tempresult 替换前类别编号
    * @return 替换后的类别标签
    * @author zhangxin
    */
  def replaceLabel(tempresult:Double): String ={
    val result=
      tempresult match {
        case 1.0 => "neg"
        case 2.0 => "neu"
        case 3.0 => "pos"
        case 4.0 => "neu_pos"  //综合类分为pos
      }
    result
  }
}
