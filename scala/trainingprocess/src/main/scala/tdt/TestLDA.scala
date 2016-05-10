package tdt

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhangxin on 2016/5/5.
  * 此类用于测试Spark 中的LDA模型，包括参数设置、模型训练、结果输出等流程
  */
object TestLDA {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("LDA").setMaster("local")
    val sc=new SparkContext(conf)

    val file="D:\\222_TDT\\data_hankcs\\datahankcs_pre_feature.txt"
    val words=sc.textFile("D:\\222_TDT\\data_hankcs\\datahankcs_dic_feature.txt").collect()

    //测试LDA全流程
//    ldaAllProcess(file,words,sc)

    //测试LDA训练+寻找最优K
    findBestK(file,words,sc)

    //测试读取模型
//    val sameModel = DistributedLDAModel.load(sc,"D:\\222_TDT\\Model\\lda.model")
//    val topTopicsPerDoc=sameModel.topTopicsPerDocument(5)
//    topTopicsPerDoc.collect().foreach(line=>{
//      println("DOC:"+line._1)
//      for(n <-Range(0,line._2.size)){
//        println("    |    "+line._2(n)+" : "+line._3(n))
//      }
//    })

  }

  /**
    * LDA全流程
    *
    * @param trainDataFile
    * @param wordsDic
    * @param sc
    *
    */
  def ldaAllProcess(trainDataFile:String,wordsDic:Array[String],sc:SparkContext): Unit ={
    //1、数据加载
    val data = sc.textFile(trainDataFile)
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // 为文档编号，编号唯一
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    //2.建立模型，参数设置，模型训练
    /**
      * k: 主题数，或者聚类中心数
      * DocConcentration：文章分布的超参数(Dirichlet分布的参数)，必需>1.0
      * TopicConcentration：主题分布的超参数(Dirichlet分布的参数)，必需>1.0
      * MaxIterations：迭代次数
      * setSeed：随机种子
      * CheckpointInterval：迭代计算时检查点的间隔
      * Optimizer：优化计算方法，目前支持"em", "online"
      */
    val k=12
    val ldaModel=new LDA().
      setK(k).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(20).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(corpus)
      .asInstanceOf[DistributedLDAModel]  //LDAModel转成其子类DistributedLDAModel
//      .asInstanceOf[LocalLDAModel]

    //3.模型输出，模型参数输出，结果输出
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
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")

    //输出每篇文章对应权重最高的主题
    //    val topicDistributionRdd = localLDA.topicDistributions(corpus)
    //    topicDistributionRdd.collect().foreach(line=>println(line._1+":  "+line._2))

    //（1）topTopicsPerDocument  每篇文档权重最高的主题。
//    val topTopicsPerDoc=ldaModel.topTopicsPerDocument(9)
//    topTopicsPerDoc.collect().foreach(line =>{
//      println(line._1+":  ")
//      for(n <- Range(0,9)){
//        println("    |    "+line._2(n)+"  "+line._3(n))
//      }
//    })

    //(2)topDocumentsPerTopic: 每个主题中权重最高的文档以及对应权重。
//    val topDocPerTopics=ldaModel.topDocumentsPerTopic(15)
//    for(topic <- Range(0,k)){
//      val temp=topDocPerTopics(topic)
//      println("【Topic " + topic + "】")
//      for(n <-Range(0,temp._1.length)){
//        println("    |    "+temp._1(n)+": "+temp._2(n))
//      }
//    }

    //（3）topicsMatrix: 主题-词分布  全矩阵输出
    val topics: Matrix = ldaModel.topicsMatrix
//    for (topic <- Range(0, k)) {
//      println("Topic " + topic + ":")
//      for (word <- Range(0, ldaModel.vocabSize)) { println("    " +wordsDic(word)+" : "+ topics(word, topic)); }
//      println()
//    }

    //4、对主题分布输出做些改进：只输出主题里权重最大的若干代表词
    for(topic <-Range(0,k)){
      val tMap=new mutable.HashMap[String,Double]()
      print("【Topic " + topic + "】")
      //①先 关键词：权重  全部放到一个map
      for(word <- Range(0, ldaModel.vocabSize)){
        tMap +=(wordsDic(word) ->topics(word, topic))
      }
      //②对map排序输出
      printSortValues(tMap,10)
      println()
    }

    //5.模型评价
//    val localModel=ldaModel.toLocal
//    val likelihood=ldaModel.logLikelihood(corpus)
//    val perplexity=ldaModel.logPerplexity(corpus)
//    println("likelihood： "+likelihood.toString)
//    println("模型评价perplexity： "+perplexity.toString)

    //6、LDA模型写出
//    ldaModel.save(sc,"D:\\222_TDT\\Model\\lda.model")

    //7.模型读取
//    val sameModel = DistributedLDAModel.load(sc,"D:\\222_TDT\\Model\\lda.model")

    //8.计算任意两个主题之间的相似度
    val averageSim=LDAOpitizer.calculateAverageSimilar(ldaModel,k)
    println("模型的平均相似度为： "+averageSim)
  }

  /**
    *输出主题模型中权重较高的若干代表词
    *
    * @param m  主题map
    * @param n  输出n个权重较大代表词
    */
  def printSortValues[T:Ordering, U:Ordering](m : mutable.HashMap[T,U],n:Int) {
    val temp= m.toArray.sortBy(_._2).toBuffer   //转成数组并根据value排序（默认从 低—>高 排序），再转成缓冲数组
    temp.trimStart(temp.size-n)              //截掉前面不需要的部分
    for(n <- Range(0,temp.size)){              //逆向输出
//      println("    |   "+temp(temp.size-n-1))
      print("   "+temp(temp.size-n-1)._1)
    }
  }

  /**
    * 模型训练
    * @param trainDataFile
    * @param wordsDic
    * @param sc
    * @param k
    * @return
    */
  def train(trainDataFile:String,wordsDic:Array[String],sc:SparkContext,k:Int): DistributedLDAModel ={

    val data = sc.textFile(trainDataFile)
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    val ldaModel=new LDA().
      setK(k).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(20).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(corpus).
      asInstanceOf[DistributedLDAModel]  //LDAModel转成其子类DistributedLDAModel
    ldaModel
  }

  //
  /**
    * 寻找最优主题数
    * @param trainDataFile  训练集
    * @param wordsDic 全词典
    * @param sc
    */
  def findBestK(trainDataFile:String,wordsDic:Array[String],sc:SparkContext): Unit ={
    var kAndAverageSim=Map[Int,Double]()
    for(k <-Range(7,35,1)){
      val ldaModel=train(trainDataFile:String,wordsDic:Array[String],sc:SparkContext,k:Int)
      val averageSim=LDAOpitizer.calculateAverageSimilar(ldaModel,k)
      println("模型的平均相似度为： "+k+" : "+averageSim)
      kAndAverageSim +=(k->averageSim)
    }
    println("【模型的平均相似度为】")
    kAndAverageSim.foreach(k=>println("  |  "+k._1+"  "+k._2))
  }
}
