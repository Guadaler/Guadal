package com.kunyandata.nlpsuit.cluster

import breeze.linalg.{*, DenseMatrix, DenseVector, diag, eig, sum}
import com.kunyandata.nlpsuit.Statistic.Similarity
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors => MVectors}
import org.apache.spark.mllib.clustering.KMeans
import scala.io.Source

/**
  * 谱聚类实现
  * Created by QQ on 5/6/16.
  */
object SpectralClustering {

  /**
    * 根据语料库，创建相关矩阵
 *
    * @param sc sparkContext
    * @param dataRDD 带有文章id的RDD，其中文章id必须从0开始，公差为1的等差递增数列
    * @return 返回一个n*n的相关矩阵，对内存的消耗大概需要 n*(4*n+4),n为词表长度
    */
  def createLaplacianMatrix(sc: SparkContext, dataRDD: RDD[(Int, Array[String])], wordListBr: Broadcast[Array[String]]): DenseMatrix[Double] = {

    // 将语料库转为文档词条矩阵
    val rowNum = dataRDD.count().toInt
    val colNum = wordListBr.value.length
    val docTermMatrixBr = sc.broadcast(DenseMatrix.zeros[Double](rowNum, colNum))
    dataRDD.foreach(line => {
      wordListBr.value.foreach(word => {

        if (line._2.contains(word)) {
          docTermMatrixBr.value(line._1, wordListBr.value.indexOf(word)) = line._2.count(_ == word)
        }

      })
    })

    // 将文档词条矩阵转化为相关矩阵
    val corrMatrixBr = sc.broadcast(DenseMatrix.zeros[Double](colNum, colNum)) // 相关矩阵的大小为n*n，n为特征长度
    sc.parallelize(wordListBr.value.indices).foreach(index => {
      val wordVector = docTermMatrixBr.value(::, index)
      val cosineDistanceArray = wordListBr.value.indices.map(tempIndex => {
        val tempWordVector = docTermMatrixBr.value(::, tempIndex)
        Similarity.cosineDistance(wordVector, tempWordVector)
      }).toArray
      val cosineDistanceVector = DenseVector(cosineDistanceArray)
      corrMatrixBr.value(index, ::) := cosineDistanceVector.t
    })

    // 计算degree matrix & laplacian matrix （L = D - A）
    val degreeMatrix = diag(sum(corrMatrixBr.value(*, ::)))

    degreeMatrix :- corrMatrixBr.value

  }

  def train(sc:SparkContext, laplacianMatrix: DenseMatrix[Double], numClusters: Int, numIterations: Int) = {

    // 特征分解
    val eig.Eig(eigenValue, _, eigenVector) = eig(laplacianMatrix)

    // 从小到大排序，获取前k个特征值对应的特征向量
    val sortedIndex = eigenValue.activeIterator.toArray.sortBy(_._2).slice(0, numClusters).map(_._1)
    var result: DenseMatrix[Double] = null
    sortedIndex.foreach(index => {

      if (result == null) {
        result = eigenVector(::, index).toDenseMatrix.t
      } else {
        result = DenseMatrix.horzcat(result, eigenVector(::, index).toDenseMatrix.t)
      }

    })

    // 建立RDD数据集，并哟个kmeans训练
    val laplacianRDD = sc.parallelize({
      val tempResult = Range(0, laplacianMatrix.rows).map(index => {
        (index, MVectors.dense(result(index, ::).t.toArray))
      })
      tempResult
    }).cache()

    val clusters = KMeans.train(laplacianRDD.map(_._2), numClusters, numIterations)
    laplacianRDD.map(x => {
      val topic = clusters.predict(x._2)
      (topic, wordsListBr.value.apply(x._1))
    }).groupByKey().foreach(println)


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

//    +++++++++++++++++++++++++++++++++++++++++++++ 谱聚类过程 ++++++++++++++++++++++++++++++++++++++++++++++++++++++++










    // 将breezeDenseMatrix转化为RDD
//    KMeans.train()

  }

}
