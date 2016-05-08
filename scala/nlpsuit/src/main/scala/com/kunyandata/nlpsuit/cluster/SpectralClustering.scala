package com.kunyandata.nlpsuit.cluster

/**
  * Created by QQ on 5/6/16.
  */

import breeze.linalg._
import breeze.linalg.eig.Eig
import com.kunyandata.nlpsuit.Statistic.Similarity
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.stat.Statistics

import scala.io.Source

object SpectralClustering {

  def countAndIndex(content:Array[String], wordList: Array[String]): Array[(Int, Double)] = {
    val result = wordList.map(word => {
      if (content.contains(word)) (wordList.indexOf(word), content.count(_ == word).toDouble)
      else (wordList.indexOf(word), 0.0)
    })
    result
  }

  // 计算相关矩阵
  def createCorrMatrix(dataRDD: RDD[String]): DenseMatrix[Double] = {

    null
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SClusterTest")
      .setMaster("local")
//      .set("spark.local.ip", "192.168.2.65")
//      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

//    ++++++++++++++++++++++++++++++++++++++ 计算 adjacency matrix ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //获取数据
//    var id = 0
//    val data = sc.parallelize(Source
//      .fromFile("D:/mlearning/trainingData/trainingWithIndus/仪电仪表")
//      .getLines().toSeq)
//      .map(line => {
//      val temp = line.split("\t")
//      val result = (id, temp(1).split(","))
//      id += 1
//      result
//    }).cache()

    val data = sc.parallelize(Seq((0, Array("a", "b", "c", "d")), (1, Array("a", "c", "d", "e")), (2, Array("b", "d", "f", "g", "k")))).cache()

    // 获得该新闻数据的词典
    val wordsList = data.map(_._2).flatMap(_.toList).collect().distinct.sorted
    val wordsListBr = sc.broadcast(wordsList)
    val docTermRowNum = data.count().toInt
    val docTermColNum = wordsList.length
    val docTermMatrixBr = sc.broadcast(DenseMatrix.zeros[Double](docTermRowNum, docTermColNum))

    data.foreach(line => {
      wordsListBr.value.foreach(word => {
        if (line._2.contains(word)) {
          docTermMatrixBr.value(line._1, wordsListBr.value.indexOf(word)) = line._2.count(_ == word)
        }
      })
    })

    println(docTermMatrixBr.value)

    val corrMatrixBr = sc.broadcast(DenseMatrix.zeros[Double](docTermColNum, docTermColNum))

    sc.parallelize(wordsListBr.value.indices).foreach(index => {
      val wordVector = docTermMatrixBr.value(::, index)
      val cosineDistanceArray = wordsListBr.value.indices.map(tempIndex => {
        val tempVector = docTermMatrixBr.value(::, tempIndex)
        Similarity.cosineDistance(wordVector, tempVector)
      }).toArray
      val cosineDistanceVector = DenseVector(cosineDistanceArray)
      corrMatrixBr.value(index, ::) := cosineDistanceVector.t
    })
    println(corrMatrixBr.value)
//    ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

//    ++++++++++++++++++++++++++++++++++++++++++ 计算degree matrix & laplacian matrix ++++++++++++++++++++++++++++++++++++++++++++++
//    L = D - A
    val degreeMatrix = diag(sum(corrMatrixBr.value(*, ::)))
    val laplacianMatrix = degreeMatrix :- corrMatrixBr.value
    val Eig(eigenValue, _, eigenVector) = eig(laplacianMatrix)
    println(eigenValue.activeIterator.toArray.sortBy(_._2).toSeq)
    println(eigenValue.activeIterator.toArray.toSeq)
    println(eigenVector.activeIterator.toArray.toSeq)



  }

}
