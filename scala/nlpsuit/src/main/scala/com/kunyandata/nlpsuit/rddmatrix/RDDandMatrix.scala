package com.kunyandata.nlpsuit.rddmatrix

import breeze.linalg.{*, DenseMatrix, DenseVector, diag, eig, sum}
import com.kunyandata.nlpsuit.Statistic.Similarity
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors => MVectors, Vector => MVector}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 5/16/16.
  */
object RDDandMatrix {

  /**
    * 创建文档词条矩阵
    *
    * @param dataRDD 数据RDD，带有新闻id，其中新闻id必须为从0开始，步长为1的等差数列
    * @param parallelism 并行程度
    * @return 返回一个矩阵，行为文档向量，列为词向量
    * @author QQ
    */
  def createDocTermRDD(sc: SparkContext, dataRDD: RDD[(Long, Array[String])],
                       parallelism: Int): RDD[(Long, Array[Int])] = {

    // 将语料库转为文档词条矩阵
    val wordlistBr = sc.broadcast(dataRDD.values.flatMap(_.toSeq).distinct().collect().sorted)
    dataRDD.values.map(content => {
      val colNum = wordlistBr.value.length
      val tempArray = new Array[Int](colNum)
      wordlistBr.value.foreach(word => {

        if (content.contains(word)) {

          val index = wordlistBr.value.indexOf(word)
          val replaceNum = content.count(_ == word)
          tempArray.update(index, replaceNum)

        } else {

          val index = wordlistBr.value.indexOf(word)
          tempArray.update(index, 0)

        }

      })

      tempArray
    }).zipWithIndex().map(line => (line._2, line._1)).repartition(parallelism)

  }

  /**
    * 创建词条文档RDD矩阵，该矩阵为文档词条矩阵的转秩
    *
    * @param sc SparkContext
    * @param dataRDD 原始RDD数据
    * @param parallelism 并行化
    * @return
    */
  def createTermDocMatrix(sc: SparkContext, dataRDD: RDD[(Long, Array[String])],
                          parallelism: Int): RDD[(Long, Array[Int])] = {


    val wordlistRDD = sc.parallelize(dataRDD.values.flatMap(_.toSeq).distinct().collect().sorted, parallelism)
      .zipWithIndex().map(line => (line._2, line._1))

    val synRDD = wordlistRDD.cartesian(dataRDD).repartition(parallelism)
    val wordCountRDD = synRDD.map(line => {
      val wordID = line._1._1
      val docID = line._2._1
      val word = line._1._2
      val content = line._2._2
      val counts = content.count(_ == word)
      (wordID, docID, counts)
    }).groupBy(_._1).map(line => (line._1, line._2.toArray.sortBy(_._2).map(_._3)))
    wordCountRDD
  }

  /**
    * 计算相关矩阵
    *
    * @param dataRDD 文档词条矩阵 (id, wordFreq)
    * @return 返回一个RDD矩阵
    * @author QQ
    */
  def createCorrRDD(dataRDD: RDD[(Long, Array[Int])],
                    parallelism: Int): RDD[(Long, Array[Double])] = {

    val corrRowRDD = dataRDD.cartesian(dataRDD).repartition(parallelism)
    val result = corrRowRDD.map(line => {
      val rowID = line._1._1
      val colID = line._2._1
      val x = line._1._2.map(_.toDouble)
      val y = line._2._2.map(_.toDouble)
      val cosDist = Similarity.cosineDistance(x, y)
      (rowID, colID, cosDist)
    }).groupBy(_._1).map(line => (line._1, line._2.toArray.sortBy(_._2).map(_._3)))
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

  /**
    * 计算任意向量的笛卡尔展开
    *
    * @param array 任意向量的List
    * @tparam T 向量中的元素的type
    * @return 返回
    */
  def cartesian[T](array: Array[T]) = {

    val result = new ArrayBuffer[(T, T)]
    array.foreach(i => {
      array.foreach(j => {
        result.append((i, j))
      })
    })

    result.filterNot(line => line._1 == line._2).toArray
  }

  /**
    * 计算一片文章中的所有词的笛卡尔积
    * @param doc 文章词向量
    * @return 返回((word A, word B), carProduct)
    */
  def cartesianProductByWordsPairs(doc: Array[String]) = {

    val docWithTf = doc.map(word => {
      (word, doc.count(_ == word))
    }).distinct

    val pairWords = cartesian(docWithTf)
    val pairWordsandProduct = pairWords.map(line => {
      val wordsPair = (line._1._1, line._2._1)
      (wordsPair, line._1._2 * line._2._2 * 1.0)
    }).filter(line => line._1._1 < line._1._2)


    // (wordsPair, product = wordsPair._1 * wordsPair._2)
    pairWordsandProduct
  }

  /**
    * 计算每一个词在一篇文章中的词频的平方
    * @param doc 文章词向量
    * @return (word A, product)
    */
  def productByWord(doc: Array[String]) = {
    val wordList = doc.distinct.sorted
    val countByWord = wordList.map(word => {
      val count = doc.count(_ == word)
      (word, count * count)
    })

    // (word, product = word * word)
    countByWord
  }

  /**
    * 基于RDD，计算余弦向量
    * @param sc SparkContext
    * @param rdd 新闻RDD
    * @return 返回的RDD中包含词和词的唯一对应关系，以及他们之间的余弦距离
    */
  def computeCosineByRDD(sc: SparkContext, rdd: RDD[Array[String]]) = {

    // 计算余弦距离的分子
    val numerator = rdd.map(cartesianProductByWordsPairs).flatMap(x => x).reduceByKey(_ + _).map(line => {
      (line._1, ("0", line._2))
    }).cache()

    // 计算余弦距离的分母
    val productByWordMap = rdd.map(productByWord).flatMap(x => x).reduceByKey(_ + _).collect().toMap
    val productByWordMapBr = sc.broadcast(productByWordMap)
    val denominator = numerator.keys.map(keysPair => {
      val n = productByWordMapBr.value(keysPair._1)
      val m = productByWordMapBr.value(keysPair._2)
      (keysPair, ("1", Math.sqrt(1.0 * n * m)))
    }).cache()

    val result = numerator.union(denominator).groupByKey.map(line => {
      val computeTemp = line._2.toMap
      val cosineDis = computeTemp("0") / computeTemp("1")
      (line._1, cosineDis)
    })

    result
  }

}
