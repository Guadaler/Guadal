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
  def createDocTermMatrix(dataRDD: RDD[(Int, Array[String])], wordListBr: Broadcast[Array[String]]): DenseMatrix[Double] = {

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

    }).collect().foreach(line => {
      val (docID, wordVector) = (line._1, line._2)
      docTermMatrix(docID, ::) := wordVector.t // 创建的向量为列向量，需要转化为行向量
    })

    docTermMatrix
  }

  /**
    * 计算相关矩阵
    * @param sc SparkContext
    * @param docTermMatrix 文档词条矩阵
    * @param wordListBr 基于dataRDD的文档词表的广播变量
    * @return 返回一个矩阵，行和列均为词与词之间的相似性
    * @author QQ
    */
  def createCorrMatrix(sc: SparkContext, docTermMatrix: DenseMatrix[Double],
                       wordListBr: Broadcast[Array[String]]): DenseMatrix[Double] = {

    val docTermMatrixBr = sc.broadcast(docTermMatrix)
    val N = wordListBr.value.length
    val corrRDD = sc.parallelize(wordListBr.value.indices).map(wordIndex => {
      val wordVector = docTermMatrixBr.value(::, wordIndex)
      val consineDistanceArray = wordListBr.value.indices.map(otherWordIndex => {
        val otherWordVector = docTermMatrixBr.value(::, otherWordIndex)
        Similarity.cosineDistance(wordVector, otherWordVector)
      }).toArray
      val consineDistanceVector = DenseVector(consineDistanceArray)
      (wordIndex, consineDistanceVector) // 这里的向量为列向量，需要转化为行向量
    }).collect()
    val corrMatrix = DenseMatrix.zeros[Double](N, N)
    corrRDD.foreach(line => {
      val (wordID, corrVector) = (line._1, line._2)
      corrMatrix(wordID, ::) := corrVector.t
    })

    corrMatrix

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

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SClusterTest")
      .setMaster("local")
//      .set("spark.local.ip", "192.168.2.65")
//      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)

//    ++++++++++++++++++++++++++++++++++++++ 计算 adjacency matrix ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //获取数据
    var id = 0
    val data = sc.parallelize(Source
//      .fromFile("/home/QQ/mlearning/trainingData/trainingWithIndus/仪电仪表")
      .fromFile("D:/纺织服装")
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
    val a = createDocTermMatrix(data, wordListBr)
    println(a)
    val b = createCorrMatrix(sc, a, wordListBr)
    println(b)
    val c = createLaplacianMatrix(b)
    println(c)
    val d = convertMatrixToRDD(sc, c)
    val kMeansModel = KMeans.train(d.map(_._2), 200, 2000)
    d.map(line => {
      val words = wordListBr.value.apply(line._1)
      (kMeansModel.predict(line._2), words)
    }).groupByKey().sortByKey().foreach(println)



  }

}
