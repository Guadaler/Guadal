package com.kunyandata.nlpsuit.util

/**
  * Created by QQ on 2016/3/18.
  */

import java.io.{FileReader, BufferedReader, PrintWriter, InputStreamReader}
import com.kunyandata.nlpsuit.classification.TrainingProcess
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

object HelloWorld {

  def readFromHDFS(path: String)={
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://222.73.34.92:9000")
    val br = new BufferedReader(new FileReader(path))
  }
  // hello world
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mltest")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/mlearning/training/wordseg_881155").collect()
    val stopWords = sc.textFile("/mlearning/stop_words_CN").collect()

    // 基于RDD的模型训练流程
    val dataRDD = sc.parallelize(data.map(line => {
      val temp = line.split("\t")
      val removedStopWords = WordSeg.removeStopWords(temp(2).split(" "), stopWords)
      Seq(temp(0), temp(1), removedStopWords.toSeq)
    }))

    val dataSets = dataRDD.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
    val dataSet = Seq(
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(4)), "test" -> dataSets(3)),
      Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(2)),
      Map("train" -> dataSets(0).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(1)),
      Map("train" -> dataSets(1).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(0))
    )

    TrainingProcess.tuneParas(dataSet, Array(1,2),
        Array(500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000,
          5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000))
  }
}
