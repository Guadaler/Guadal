package com.kunyandata.nlpsuit.cluster

import breeze.linalg.{*, DenseMatrix, DenseVector, diag, eig, sum}
import com.kunyandata.nlpsuit.Statistic.Similarity
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors => MVectors, Vector => MVector}
import org.apache.spark.mllib.clustering.KMeans
import scala.collection.Parallelizable
import scala.io.Source

/**
  * 谱聚类实现
  * Created by QQ on 5/6/16.
  */
object SpectralClustering {

  /**
    * 创建文档词条矩阵
    *
    * @param dataRDD 数据RDD，带有新闻id，其中新闻id必须为从0开始，步长为1的等差数列
    * @param wordListBr 基于dataRDD的文档词表的广播变量
    * @return 返回一个矩阵，行为文档向量，列为词向量
    * @author QQ
    */
  def createDocTermRDD(dataRDD: RDD[(Int, Array[String])],
                          wordListBr: Broadcast[Array[String]]): RDD[(Int, Array[Int])] = {

    // 获取词向量长度
    val colNum = wordListBr.value.length

    // 将语料库转为文档词条矩阵
    dataRDD.map(line => {
      val (docID, content) = (line._1, line._2)
      val tempArray = Array[Int](colNum)
      wordListBr.value.foreach(word => {

        if (content.contains(word)) {
          tempArray.update(wordListBr.value.indexOf(word), content.count(_ == word))
        }

      })

      (docID, tempArray)
    })

  }

  /**
    * 计算相关矩阵
    *
    * @param docTermRDD 文档词条矩阵 (id, wordFreq)
    * @param wordListBr 基于dataRDD的文档词表的广播变量
    * @return 返回一个RDD矩阵
    * @author QQ
//    */
  def createCorrRDD(docTermRDD: RDD[(Int, Array[Int])],wordListBr: Broadcast[Array[String]],
                    parallelizim: Int): RDD[(Long, Iterable[(Long, Long, Double)])] = {

    val selfWithID = docTermRDD.values.map(_.map(_.toDouble)).zipWithIndex()
    val corrRowRDD = selfWithID.cartesian(selfWithID).repartition(parallelizim)
    val result = corrRowRDD.map(line => {
      val rowID = line._1._2
      val colID = line._2._2
      val x = line._1._1
      val y = line._2._1
      val cosDist = Similarity.cosineDistance(x, y)
      (rowID, colID, cosDist)
    }).groupBy(_._1)
    result
  }


  /**
    * 计算laplace矩阵
    *
    * @param corrMatrix 相关矩阵
    * @return laplace矩阵
    * @author QQ
    */
  def createLaplacianMatrix(corrMatrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    val degreeMatrix = diag(sum(corrMatrix(*, ::)))

    degreeMatrix :- corrMatrix
  }

  /**
    * 矩阵特征分解
    *
    * @param laplacianMatrix laplace矩阵
    * @param k 降维数目
    * @return 特征分解后组成的矩阵，其中行为词向量，corrMatrix的特征向量的子集（从小到大排序，取前k个）
    * @author QQ
    */
  def eigenValueDecomposeToMatrix(laplacianMatrix: DenseMatrix[Double], k: Int): DenseMatrix[Double] = {

    val eig.Eig(eigenValue, _ , eigenVectors) = eig(laplacianMatrix)
    val sortedIndex = eigenValue.activeIterator.toArray.sortBy(_._2).slice(0, k).map(_._1)
    var result: DenseMatrix[Double] = null
    sortedIndex.foreach(index => {

      if (result == null) {
        result = eigenVectors(::, index).toDenseMatrix.t
      } else {
        result = DenseMatrix.horzcat(result, eigenVectors(::, index).toDenseMatrix.t)
      }

    })

    result
  }

  /**
    * 将矩阵转化为RDD
    *
    * @param sc SparkContext
    * @param matrix 矩阵
    * @return RDD，包含id（此处id为wordlist中的索引）和scala.mllib.linalg.Vector
    * @author QQ
    */
  def convertMatrixToRDD(sc:SparkContext, matrix: DenseMatrix[Double]): RDD[(Int, MVector)] = {
    val lapacianRDD = sc.parallelize({
      val temp = Range(0, matrix.rows).map(rowID => {
        (rowID, MVectors.dense(matrix(rowID, ::).t.toArray))
      })
      temp
    }).cache()

    lapacianRDD
  }

  /**
    * 将RDD转化为矩阵
    *
    * @param rdd 元素为densevector且所有元素长度一致的rdd
    * @return 返回一个DenseMatrix
    * @author QQ
    */
  def convertRDDToMatrix(rdd: RDD[(Int, DenseVector[Double])]): DenseMatrix[Double] = {
    val rowNum = rdd.count().toInt
    val colNum = rdd.map(_._2.length).max()
    val resultMatrix = DenseMatrix.zeros[Double](rowNum, colNum)
    rdd.collect().foreach(row => {
      val (rowID, vectors) = (row._1, row._2)
      resultMatrix(rowID, ::) := vectors.t
    })

    resultMatrix
  }
}
