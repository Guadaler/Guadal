package com.kunyandata.nlpsuit.sentiment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2016/3/9.
  */
object Action_mllib {
  def main(args: Array[String]) {

      // 屏蔽不必要的日志显示在终端上
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      //spark程序入口
      var conf=new SparkConf().setMaster("local").setAppName("testSvm")
      var sc=new SparkContext(conf)

      System.setProperty("hadoop.home.dir", "D:/hadoop");  //报Hadoop的错误

//      var data=MLUtils.loadLibSVMFile(sc,"src/main/resource/sample_binary_classification_data.txt")
      var data=MLUtils.loadLibSVMFile(sc,"src/main/resource/svm_jingdong_4000.txt")
//      var data=MLUtils.loadLibSVMFile(sc,"src/main/resource/svm_jingdong_500.txt")
      var splits=data.randomSplit(Array(0.8,0.2))
      var train=splits(0).cache()
      var test=splits(1).cache()
      val itraterNum=100

      //训练
      var svmtest=new SVMWithSGD()
      var model=svmtest.run(train)

      //  预测
      val scoreandLabels=test.map{point=>
          var score=model.predict(point.features)
         (score,point.label)
      }

      //性能评价
      var metrics=new BinaryClassificationMetrics(scoreandLabels)
      var auROC=metrics.areaUnderROC()
      println("Under ROC=: "+auROC)

      //统计分类错误
      val trainErr = scoreandLabels.filter(r => r._1 != r._2).count.toDouble / data.count
      println("Training Error = " + trainErr)

      //计算模型精度
      val metrics2 = new MulticlassMetrics(scoreandLabels)
      val precision = metrics2.precision
      println("precision:" + precision)

      sc.stop()


      //模型保存
//      var modelPath="src/main/resource/model2.txt"
//      model.save(sc,modelPath)
//
//      println("【调用现成的model进行预测】:"+modelPath)
//      if(sc==null){
//          println("sc为空")
//      }else{
//          println("sc不为空")
//      }
//
//      var model2=SVMModel.load(sc,modelPath)
//
//      var scoreandLabel2=test.map{point =>
//          var score=model2.predict(point.features)
//          (score,point.label)
//      }
//      //性能评价
//      var metrics2=new BinaryClassificationMetrics(scoreandLabel2)
//      var auROC2=metrics2.areaUnderROC()
//      println("Under ROC=: "+auROC2)

  }
}
