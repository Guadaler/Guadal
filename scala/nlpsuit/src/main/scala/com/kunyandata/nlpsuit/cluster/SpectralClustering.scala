package com.kunyandata.nlpsuit.cluster

/**
  * Created by QQ on 5/6/16.
  */

import breeze.linalg
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.linalg.eig.Eig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.stat.Statistics

import scala.io.Source

object SpectralClustering {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SClusterTest")
      .setMaster("local")
      .set("spark.local.ip", "192.168.2.65")
      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

    //获取数据
    val data = sc.parallelize(Source
      .fromFile("/home/QQ/Documents/trainingWithIndus/仪电仪表")
      .getLines()
      .toSeq).map(line => {
      val Array(label, content) = line.split("\t")
      (label, content.split(","))
    })

    // 获得词序列
    val wordsList = data.map(_._2).flatMap(_.toList).collect().distinct.sorted
    println(wordsList.length)
    val temp = DenseVector.zeros[Double](wordsList.length)

    val docTermMatrix = data.map(line => {
      wordsList.foreach(word => {
        if(line._2.contains(word)) {
          temp(wordsList.indexOf(word)) = wordsList.count(_ == word)
        }
      })
      temp
    }).collect()
    val a = DenseMatrix(docTermMatrix)
    val b = DenseMatrix(Array(1.0,2.0), Array(3.0,4.0))


    println(a)
    println(b)


  }

}
