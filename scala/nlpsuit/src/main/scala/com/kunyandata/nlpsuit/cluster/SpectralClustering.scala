package com.kunyandata.nlpsuit.cluster

/**
  * Created by QQ on 5/6/16.
  */

import breeze.linalg.{*, DenseMatrix, DenseVector, diag, eig, sum}
import com.kunyandata.nlpsuit.Statistic.Similarity
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors => MVectors}
import org.apache.spark.mllib.clustering.KMeans
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
  def createCorrMatrix(sc:SparkContext, dataRDD: RDD[String]): DenseMatrix[Double] = {

    null
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SClusterTest")
      .setMaster("local")
      .set("spark.local.ip", "192.168.2.65")
      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

//    ++++++++++++++++++++++++++++++++++++++ 计算 adjacency matrix ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //获取数据
    var id = 0
    val data = sc.parallelize(Source
//      .fromFile("/home/QQ/mlearning/trainingData/trainingWithIndus/仪电仪表")
      .fromFile("/home/QQ/Documents/trainingWithIndus/仪电仪表")
      .getLines().toSeq)
      .map(line => {
      val temp = line.split("\t")
      val result = (id, temp(1).split(","))
      id += 1
      result
    }).cache()

//    val data = sc.parallelize(Seq((0, Array("a", "b", "c", "d")), (1, Array("a", "c", "d", "e")), (2, Array("b", "d", "f", "g", "k")))).cache()

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
//    ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

//    ++++++++++++++++++++++++++++++++++++++++++ 计算degree matrix & laplacian matrix ++++++++++++++++++++++++++++++++++
//    L = D - A
    val degreeMatrix = diag(sum(corrMatrixBr.value(*, ::)))
    val laplacianMatrix = degreeMatrix :- corrMatrixBr.value
    val eig.Eig(eigenValue, _, eigenVector) = eig(laplacianMatrix)
    println(eigenValue)
    println(eigenVector)

//    ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

//    +++++++++++++++++++++++++++++++++++++++++++++ 谱聚类过程 ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    val k = 50
    val resultIndex = eigenValue.activeIterator.toArray.sortBy(_._2).slice(0, k).map(_._1)
    println(resultIndex.toSeq)
    var result: DenseMatrix[Double] = null
    resultIndex.foreach(index => {
      if (result == null) {
        result = eigenVector(::, index).toDenseMatrix.t
      } else {
        result = DenseMatrix.horzcat(result, eigenVector(::, index).toDenseMatrix.t)
      }
    })
    println(result)

    val matrixRDD = sc.parallelize({
      var id = -1
      val tempResult = Range(0, docTermColNum).map(index => {
        id += 1
        (id, MVectors.dense(result(index, ::).t.toArray))
      })
      tempResult
    }).cache()

    val numClusters = k
    val numIterations = 2000
    val clusters = KMeans.train(matrixRDD.map(_._2), numClusters, numIterations)
    matrixRDD.map(x => {
      val topic = clusters.predict(x._2)
      (topic, wordsListBr.value.apply(x._1))
    }).groupByKey().foreach(println)







    // 将breezeDenseMatrix转化为RDD
//    KMeans.train()

  }

}
