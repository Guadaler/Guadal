package com.kunyan.tdt.process

import com.kunyan.tdt.util.HBaseUtil
import com.kunyandata.nlpsuit.rddmatrix.RDDandMatrix
import com.kunyandata.nlpsuit.util.{JsonConfig, KunyanConf, TextPreprocessing}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * Created by QQ on 6/29/16.
  */
object StepOne {

  def getAndFormatData(sc: SparkContext, jsonConfig: JsonConfig, kunyanConf: KunyanConf) = {

    // 获取hbase配置信息
    val hbaseConfig = HBaseUtil.getHbaseConf(
      jsonConfig.getValue("hbase", "rootDir"),
      jsonConfig.getValue("hbase", "ip"))

    // 获取停用词表
    val stopWordsCNbr = sc.broadcast(Source.fromFile(jsonConfig.getValue("tdt", "stopWordsPath")).getLines().toArray)

    // 获取新闻数据
    val newsData = HBaseUtil.getRDD(sc, hbaseConfig)//.foreach(println)
    // sc.parallelize(HBaseUtil.getRDD(sc, hbaseConfig).take(100))

    // 数据清洗，并分词
    val wordsRDD = newsData.map(_.split("\n\t")).filter(_.length == 3).map(line => {
      val Array(_, _, content) = line
      TextPreprocessing.process(content, stopWordsCNbr.value, kunyanConf)
    })

    wordsRDD

    //    // 获取需要筛选的词性
    //    val filterTag = sc.broadcast(Source.fromFile(jsonConfig.getValue("tdt", "wordsTag"))
    //      .getLines().map(_.split(" ")(0)).toArray)
    //
    //    // 获取新闻数据
    //    val newsData = HBaseUtil.getRDD(sc, hbaseConfig)
    //
    //    val data = newsData.map(_.split("\n\t")).filter(_.length == 3).map(line => {
    //      val tempContent = line(2)
    //      val wordsWithTag = AnsjAnalyzer.cutWithTag(tempContent)
    //      val k = wordsWithTag.filter(word => {
    //        filterTag.value.contains(word.getNatrue.natureStr)
    //      }).map(_.getName)
    //
    //      k
    //    })
    //
    //    data
  }

  def makeDictsWithIndex(sc: SparkContext, rdd: RDD[Array[String]]) = {

    rdd.flatMap(x => x).distinct().zipWithUniqueId().collect().toMap
  }

  def computeDistance(sc: SparkContext,
                      rdd: RDD[Array[String]],
                      wordsWithIndex: Map[String, Long],
                      jsonConfig: JsonConfig) = {

    val result = RDDandMatrix.computeCosineByRDD(sc, rdd,
      jsonConfig.getValue("tdt", "support").toInt,
      jsonConfig.getValue("tdt", "partition").toInt,
      jsonConfig.getValue("tdt", "shufflePartition").toInt).map(line => {
      ((wordsWithIndex(line._1._1), wordsWithIndex(line._1._2)), (line._2._1, Math.round(line._2._2 * 100000) + 1))
    })

    // ((word, word), (support, distance))
    result
  }

}
