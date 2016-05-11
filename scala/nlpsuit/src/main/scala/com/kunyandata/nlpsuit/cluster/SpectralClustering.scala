package com.kunyandata.nlpsuit.cluster

import breeze.linalg.{*, DenseMatrix, DenseVector, diag, eig, sum}
import com.kunyandata.nlpsuit.Statistic.Similarity
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors => MVectors, Vector => MVector}
import org.apache.spark.mllib.clustering.KMeans
import scala.io.Source

/**
  * 谱聚类实现
  * Created by QQ on 5/6/16.
  */
object SpectralClustering {

  /**
    * 创建文档词条矩阵
    * @param dataRDD 数据RDD，带有新闻id，其中新闻id必须为从0开始，步长为1的等差数列
    * @param wordListBr 基于dataRDD的文档词表的广播变量
    * @return 返回一个矩阵，行为文档向量，列为词向量
    * @author QQ
    */
  def createDocTermRDD(dataRDD: RDD[(Int, Array[String])],
                          wordListBr: Broadcast[Array[String]]): RDD[(Int, DenseVector[Double])] = {

    // 创建值为0，行为文本数量，列为词汇表长度的空矩阵
    val rowNum = dataRDD.count().toInt
    val colNum = wordListBr.value.length
    val docTermMatrix = DenseMatrix.zeros[Double](rowNum, colNum)

    // 将语料库转为文档词条矩阵
    dataRDD.map(line => {
      val (docID, content) = (line._1, line._2)
      val tempVector = DenseVector.zeros[Double](colNum)
      wordListBr.value.foreach(word => {

        if (content.contains(word)) {
          tempVector(wordListBr.value.indexOf(word)) = content.count(_ == word)
        }

      })

      (docID, tempVector)

    })

  }

  /**
    * 计算相关矩阵
    * @param sc SparkContext
    * @param docTermRDD 文档词条矩阵
    * @param wordListBr 基于dataRDD的文档词表的广播变量
    * @return 返回一个矩阵，行和列均为词与词之间的相似性
    * @author QQ
    */
  def createCorrRDD(sc: SparkContext, docTermRDD: RDD[DenseVector[Double]],
                       wordListBr: Broadcast[Array[String]]): RDD[(Int, DenseVector[Double])] = {

    val N = wordListBr.value.length
    val docTermArray = docTermRDD.collect()
    val corrRDD = sc.parallelize(wordListBr.value.indices).map(wordIndex => {

      // 根据wordIndex获取列向量
      val wordVector = DenseVector(docTermArray.map(vectors => vectors(wordIndex)))

      // 分别计算该列向量和其他列向量之间的余弦距离，并得到一个值为余弦距离的向量
      val consineDistanceArray = wordListBr.value.indices.map(otherWordIndex => {
        val otherWordVector = DenseVector(docTermArray.map(vectors => vectors(wordIndex)))
        Similarity.cosineDistance(wordVector, otherWordVector)
      }).toArray
      val consineDistanceVector = DenseVector(consineDistanceArray)
      (wordIndex, consineDistanceVector)
    })

    corrRDD

  }


  /**
    * 计算laplace矩阵
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

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SClusterTest")
      .setMaster("local")
//      .set("spark.local.ip", "192.168.2.65")
      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

//    ++++++++++++++++++++++++++++++++++++++ 计算 adjacency matrix ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //获取数据
    var id = 0
    val data = sc.parallelize(Source
      .fromFile("/home/QQ/Documents/trainingWithIndus/仪电仪表")
//      .fromFile("D:/纺织服装")
      .getLines().toSeq)
      .map(line => {
      val temp = line.split("\t")
      val result = (id, temp(1).split(","))
      id += 1
      result
    }).cache()

//    val data = sc.parallelize(Seq((0, Array("a", "b", "c", "d")), (1, Array("a", "c", "d", "e")), (2, Array("b", "d", "f", "g", "k")))).cache()

    val wordList = data.map(line => line._2).flatMap(_.toSeq).distinct().collect().sorted
    val wordListBr = sc.broadcast(wordList)
    val aRDD = createDocTermRDD(data, wordListBr)
    val bRDD = createCorrRDD(sc, aRDD.map(_._2), wordListBr)

  }
}
