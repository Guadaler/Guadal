package com.kunyandata.nlpsuit.sentiment

import java.io.{PrintWriter, File, FileInputStream, ObjectInputStream}

import com.kunyandata.nlpsuit.util.TextProcess
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{ChiSqSelectorModel, HashingTF, IDFModel}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by zx on 2016/3/28.
  */
object NB_predict extends App{

  val conf =new SparkConf().setAppName("test").setMaster("local")
  val sc=new SparkContext(conf)

  val modelMap_f=init("D:\\000_DATA\\Model\\【第五次标注】\\F_2")
  val modelMap_s=init("D:\\000_DATA\\Model\\【第五次标注】\\S")
  val model=Array[Map[String, Any]](modelMap_f,modelMap_s)

  val stopWords=Source.fromFile("D:\\111_DATA\\data\\stop_words_CN").getLines().toArray  //读取停用词典并转成Array
  val stopWordsBr = sc.broadcast(stopWords)

  //单模型+单篇文章
  val content = "本期高频数据显示：1月信贷扩张剧烈达历年之最，同时受人民币外汇市场波动与疲弱出口影响，宽口径外汇占款下降显著，2月上旬央行货币净投放更创历史新高。\n　　信贷极度宽松背景下利率市场表现稳定，只有信托类产品收益率在2月上旬显著下降。信贷扩张还未传导至实体经济，其中工业方面钢铁行业去产能进一步加速并为价格托底，煤炭、水泥价格依旧徘徊谷底；航运价格仍持续走低；受春节假期影响新房、二手房与土地交易量均表现惨淡。\n　　2月5日当周央行公开市场货币净投放为3300亿元，尽管比上期6900亿有所下降，但仍保持历史高位。2月上旬银行间市场隔夜回购利率上升29个基点至2.26%，7天回购利率下降4个基点至3.06%。2月上旬国债收益率基本维持不变，AAA级与AA级企业债收益率分别上升2个基点。AA+级收益率小幅下降。\n　　钢铁的消费中，基建和房地产超过50%，机械和汽车接近25%，所以钢铁的供求和价格数据是观测固定资产投资和消费的重要指标。钢铁去产能步伐加速，价格回落。1月下旬粗钢产量同比增长-10.81%，连续5旬出现两位数负增长。2月上旬唐山钢坯价格维持在1540元/吨，为近一个月来新低。1月出口（除中国香港地区外）同比增长-12.33%，是近10个月来最大负增长。\n　　1月份新增社会融资总额34173亿元，环比猛增88%，增长主要来源于新增人民币贷款项（同比增长72%，达到25370亿元）。\n　　1月金融机构新增外汇占款减少23766亿元，达到11年来的最大当月减少值。1月中国百城新建住宅价格上升0.42%，其中一、二、三线城市环比分别上升1.89%、0.80%、-1.08%。2014年房地产投资占GDP14.9%，占固定资产投资的18.6%，房地产贷款余额占总贷款余额比21.27%，土地出让金占政府收入30.4%。"
  val result =predict(content, modelMap_f, stopWordsBr)
  println("预测结果为： "+result)

  //二级模型+批量文章
  val filePredictPath="D:\\000_DATA\\predictTest\\predictData"
  val outPredictPath="D:\\000_DATA\\predictTest\\predictResult.txt"
  predictMany(filePredictPath,outPredictPath,model,stopWordsBr)


  /**
    * 初始化读取模型
    *
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
    * 情感预测  单层模型+单篇文章
    * @param content  待预测文章
    * @param models  模型Map[模型名称，模型]，由init初始化得到
    * @param stopWordsBr 停用词
    * @return  返回情感label
    */
  def predict(content: String, models: Map[String, Any], stopWordsBr: Broadcast[Array[String]]): Double = {
    val wordSegNoStop = TextProcess.process(content, stopWordsBr,0)
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))
    prediction
  }

  /**
    * 二级模型 +单篇文章
    * @param content  文章内容
    * @param arr  二级模型数组
    * @param stopWordsBr  停用词表
    * @return 预测label
    */
  def predictOne(content:String,arr:Array[Map[String, Any]],stopWordsBr: Broadcast[Array[String]]): String ={
    var temp = predict(content,arr(0), stopWordsBr)
    if (temp == 4.0) {
      temp = predict(content,arr(1), stopWordsBr)
    }
    val result=replaceLabel(temp)
    result
  }

  /**
    * 二级模型，批量预测
    * @param filepath  批量预测文章路径
    * @param outpath  输出预测结果
    * @param arr  二级模型数组
    * @param stopWordsBr  停用词表
    */
  def predictMany(filepath:String,outpath:String,arr:Array[Map[String, Any]],stopWordsBr: Broadcast[Array[String]]): Unit ={
    val wr=new PrintWriter(outpath,"utf-8")
    val files=new File(filepath).listFiles()
    for(file <-files) {
      val title = file.getName.substring(0, file.getName.indexOf(".txt"))
      var contentstr = ""
      for (line <- Source.fromFile(file).getLines()) {
        contentstr += line
      }
      var temp = predict(contentstr,arr(0), stopWordsBr)
      if (temp == 4.0) {
        temp = predict(contentstr,arr(1), stopWordsBr)
      }
      val result=replaceLabel(temp)
      wr.write("【" + result + "】" + title + "\n")
      wr.flush()
    }
  }

  /**
    * 编号替换成标签  如 1.0 =》 neg
    * @param tempresult
    * @return
    */
  def replaceLabel(tempresult:Double): String ={
    val result=
      tempresult match {
        case 1.0 => "neg"
        case 2.0 => "neu"
        case 3.0 => "pos"
      }
    result
  }
}
