package com.kunyandata.nlpsuit.util

/**
  * Created by QQ on 2016/3/18.
  */

import com.kunyandata.nlpsuit.classification.TrainingProcess
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  // hello world
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mltest")
    val sc = new SparkContext(conf)
    val data = sc.textFile("D:/mlearning/training/wordseg_881155")
    val stopWords = sc.textFile("D:/mlearning/dicts/stop_words_CN").collect()
    val stopWordsBr = sc.broadcast(stopWords)

    // 基于RDD的模型训练流程
    val dataRDD = data.map(line => {
      val temp = line.split("\t")
      (if(temp(0) == "881155") 1.0 else 0.0, temp(1).split(","))
    })

    val dataSets = dataRDD.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
    val dataSet = Seq(
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(4)), "test" -> dataSets(3)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(2)),
      Map("train" -> dataSets(0).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(1)),
      Map("train" -> dataSets(1).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(0))
    )

    TrainingProcess.tuneParas(dataSet, Array(1),
        Array(300))
    sc.stop()
  }
}
