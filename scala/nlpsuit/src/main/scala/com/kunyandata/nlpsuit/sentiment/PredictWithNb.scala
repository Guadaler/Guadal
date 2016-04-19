package com.kunyandata.nlpsuit.sentiment

import java.io.{PrintWriter, File, FileInputStream, ObjectInputStream}

import com.kunyandata.nlpsuit.util.TextPreprocessing
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{ChiSqSelectorModel, HashingTF, IDFModel}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by zx on 2016/3/28.
  */
object PredictWithNb extends App{

  /**
    * 初始化读取模型  给出模型路径
    * @param path  模型路径
    * @return 模型Map[模型名称，模型]
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
    *初始化读取模型  调用默认路径的模型
    * @return  模型数组
    */
  def init(): Map[String, Any] = {
//    val path = "text_classification/1.1/Spark_NLP_suit/src/main/resources/sentimodels"  //模型路径  三分类模型
    val path = "src/main/resources/sentimodels/F_2_1500"  //模型路径  二分类（负类和其他）
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
 *
    * @param content  待预测文章
    * @param models  模型Map[模型名称，模型]，由init初始化得到
    * @param stopWordsBr 停用词
    * @param typ 分词模式  0 和1
    * @return  返回情感label编号
    */
  def predict(content: String, models: Map[String, Any], stopWordsBr: Broadcast[Array[String]],typ: Int): Double = {
    val wordSegNoStop = TextPreprocessing.process(content, stopWordsBr,typ)
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
    * @param stopWordsBr 停用词
    * @return  返回情感label编号
    */
  def predict(content: String, models: Map[String, Any], stopWordsBr: Broadcast[Array[String]]): Double = {
    val wordSegNoStop = TextPreprocessing.process(content, stopWordsBr)
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))
    prediction
  }

  /**
    * 单模型+单篇文章 +坤雁分词
    * @param content 文章内容
    * @param model 模型
    * @param stopWordsBr 停用词
    * @return  情感label
    * @author zhangxin
    */
  def predictWithSigle(content:String,model:Map[String, Any],stopWordsBr: Broadcast[Array[String]],typ:Int): String ={
    val temp = predict(content,model, stopWordsBr,typ: Int)
    val result=replaceLabel(temp)
    result
  }
  /**
    * 用ansj分词
    */
  def predictWithSigle(content:String,model:Map[String, Any],stopWordsBr: Broadcast[Array[String]]): String ={
    val temp = predict(content,model, stopWordsBr)
    val result=replaceLabel(temp)
    result
  }

  /**
    * 二级模型 +单篇文章
    * @param content  文章内容
    * @param arr  二级模型数组
    * @param stopWordsBr  停用词表
    * @return 情感label
    * @author zhangxin
    */
  def predictWithFS(content:String,arr:Array[Map[String, Any]],stopWordsBr: Broadcast[Array[String]],typ: Int): String ={
    var temp = predict(content,arr(0), stopWordsBr,typ: Int)
    if (temp == 4.0) {
      temp = predict(content,arr(1), stopWordsBr,typ: Int)
    }
    val result=replaceLabel(temp)
    result

    /*val result=if(predict(content,arr(0), stopWordsBr,typ: Int) ==0.4){
      replaceLabel(predict(content,arr(0), stopWordsBr,typ: Int))
    }else{
      replaceLabel(predict(content,arr(1), stopWordsBr,typ: Int))
    }
    result*/
  }

  /**
    * 二级模型，批量预测
    * @param filepath  批量预测文章路径
    * @param outpath  输出预测结果
    * @param arr  二级模型数组
    * @param stopWordsBr  停用词表
    * @author zhangxin
    */
  def predictManyWithFS(filepath:String,outpath:String,arr:Array[Map[String, Any]],stopWordsBr: Broadcast[Array[String]],typ: Int): Unit ={
    val wr=new PrintWriter(outpath,"utf-8")
    val files=new File(filepath).listFiles()
    for(file <-files) {
      val title = file.getName.substring(0, file.getName.indexOf(".txt"))
      var contentstr = ""
      for (line <- Source.fromFile(file).getLines()) {
        contentstr += line
      }
      var temp = predict(contentstr,arr(0), stopWordsBr,typ: Int)
      if (temp == 4.0) {
        temp = predict(contentstr,arr(1), stopWordsBr,typ: Int)
      }
      val result=replaceLabel(temp)
      wr.write("【" + result + "】" + title + "\n")
      wr.flush()
    }
  }

  /**
    * 编号替换成标签  如 1.0 =》 neg
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
