package LDA

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDAModel, LocalLDAModel}
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.rdd.RDD

/**
  * Created by zhangxin on 2016/5/10.
  * 本类主要用于模型相似度计算 ，参数K优化 ，部分工具方法 ，结果输出
  */
object UtilLDA {

  /**
    * 基于余弦定理的主题平均相似度计算
    *
    * @param ldaModel  LDA模型
    * @param k 主题数量
    * @return 该模型的平均相似度
    * @author zhangxin
    */
  def calculateAverageSimilar(ldaModel: LDAModel, k: Int): Double = {

    val topics: Matrix = ldaModel.topicsMatrix
    var sum = 0.0

    //计算任意两个主题的相似度，并求和
    for(i <- Range(0, k)){

      var fenzi = 0.0  //分子
      var fenmu = 0.0  //分母


      for(j <- Range(i+1, k)){

        var x = 0.0
        var y = 0.0

        for(word <- Range(0, ldaModel.vocabSize)){

          val topic_i = topics(word, i)
          val topic_j = topics(word, j)
          fenzi += topic_i*topic_j

          x += scala.math.pow(topic_i, 2)
          y += scala.math.pow(topic_j, 2)

        }

        fenmu = scala.math.sqrt(x*y)

        //计算主题i和主题j的相似度
        var similar = fenzi/fenmu

        //叠加求和
        sum += similar
      }
    }

    //计算平均相似度
    val averageSimilar = 2*sum/(k*(k-1))

    averageSimilar
  }

  /**
    * 在指定范围和步长内寻找最优主题K
    *
    * @param sc sc
    * @param trainData 训练集
    * @param modelArgs 模型参数
    * @param begin 范围起
    * @param end 范围止
    * @param step 步长
    * @return 最优k和平均相似度
    */
  def findBestK(sc: SparkContext, trainData: RDD[(Long, Vector)],
                modelArgs: Array[Int], begin: Int, end: Int, step: Int ) = {

    var kAndAverageSim = Map[Int, Double]()

    //循环进行基于不同K的训练
    for(k <- Range(begin, end, step)){

      modelArgs(0) = k   //更新参数数组中的K值
      val ldaModel = Train.trainWithEM(sc, modelArgs, trainData)  //调用Train类中的trainWithEM方法
      val averageSim = UtilLDA.calculateAverageSimilar(ldaModel, k)
      kAndAverageSim += (k -> averageSim)

    }

    println("【模型的平均相似度为】")
    kAndAverageSim.foreach(k => println("  |  "+k._1+"  "+k._2))

    //返回最佳K 及模型平均相似度
    var bestK = 0
    var averageSim = 99D
    kAndAverageSim.foreach(k => {

      if(k._2.toDouble < averageSim){
        bestK = k._1
        averageSim = k._2
      }

    })

    (bestK, averageSim)
  }

  //-------------------【结果输出】-------------------------
  /**
    * EMLDAOptimizer生成的结果为DistributedLDAModel类, 后者不仅存储了推断的主题，还有整个训练集的主题分布。
    * DistributedLDAModel提供:
    * topTopicsPerDocument: 训练集中每篇文档权重最高的主题。
    * topDocumentsPerTopic: 每个主题中权重最高的文档以及对应权重。
    * logPrior: 根据模型分布计算的log probability。
    * logLikelihood: 根据训练集的模型分布计算的log likelihood。
    * topicDistributions: 训练集中每篇文档的主题分布，相当于theta。
    * topicsMatrix: 主题-词分布，相当于phi。
    * vocabSize：单词数目 （也即VSM维度）
    */

  /**
    * 输出主题模型中权重较高的若干代表词
    *
    * @param ldaModel lda模型
    * @param vocabArray 词表
    * @param k 主题K
    * @author zhangxin
    */
  def printTopWordsPerTopics(ldaModel: LDAModel, vocabArray: Array[String], k:Int): Unit = {

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)

    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabArray(_)).zip(termWeights)
    }

    topics.zipWithIndex.foreach { case (topic, i) =>
      println("TOPIC " + i)
      topic.foreach { case (term, weight) => println(term+" : "+weight) }
      println(s"==========")
    }

  }

  /**
    * 每篇文档权重最高的主题n
    *
    * @param ldaModel lda模型
    * @param n 主题n  n<=k
    * @author zhangxin
    */
  def printTopTopicsPerDocument(ldaModel: DistributedLDAModel, n: Int): Unit = {

    val topTopicsPerDoc = ldaModel.topTopicsPerDocument(n)

    topTopicsPerDoc.collect().foreach(line => {

      println(line._1 + ":  ")
      for(n <- Range(0, 9)){
        println("    |    "+line._2(n)+"  "+line._3(n))
      }

    })
  }

  /**
    * 每个主题中权重最高的文档以及对应权重
    * @param ldaModel lda模型
    * @param n 文档数n  n<=总文档数
    * @author zhangxin
    */
  def printTopDocumentsPerTopic(ldaModel: DistributedLDAModel, n: Int): Unit = {

    val topDocPerTopics = ldaModel.topDocumentsPerTopic(n)

    for(topic <- Range(0,ldaModel.k)){

      val temp = topDocPerTopics(topic)
      println("[Topic " + topic + "]")

      for(n <- Range(0, temp._1.length)){
        println("    |    "+temp._1(n)+": "+temp._2(n))
      }

    }
  }


  /**
    * 主题-词分布  全矩阵输出
    * @param ldaModel lda模型
    * @param wordsDic 词典
    * @author zhangxin
    */
  def printMatrix(ldaModel: DistributedLDAModel, wordsDic: Array[String]): Unit = {

    val topics: Matrix = ldaModel.topicsMatrix

    for (topic <- Range(0, ldaModel.k)) {

      println("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { println("    " +wordsDic(word)+" : "+ topics(word, topic))}
      println()

    }

  }

  //-----------模型评价--------------------------------
  /**
    * 模型评价，注意只有localModel才有困惑度
    * @param ldaModel lda模型
    * @param trainData 数据集
    * @author zhangxin
    */
  def printPlexity(ldaModel: LocalLDAModel, trainData: RDD[(Long,Vector)]): Unit = {
    val likelihood=ldaModel.logLikelihood(trainData)
    val perplexity=ldaModel.logPerplexity(trainData)
    println("likelihood： "+likelihood.toString)
    println("perplexity： "+perplexity.toString)

  }

  //--------------模型更新--------------------------
  /**
    * 删除指定路径下的模型，删除模型的所有文件
    * @param path  模型路径
    * @author zhangxin
    */
  def deleteModel(path: String): Unit = {

    //根路径
    val root = new File(path)

    def deleteAll(root: File): Unit = {

      if (root.isDirectory) {

        //如果当前路径是文件夹，则获取所有文件夹
        val templist = root.listFiles()

        templist.foreach(line => {
          deleteAll(line)
          //递归删除其所有子文件以后再删除当前文件夹
          line.delete()
        })

      } else {
        //如果当前路径不是文件夹，则直接删除
        root.delete()
      }

    }

    deleteAll(root)
  }

}
