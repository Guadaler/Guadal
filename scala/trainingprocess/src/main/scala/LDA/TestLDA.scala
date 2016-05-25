package LDA

import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangxin on 2016/5/10.
  * LDA模型训练测试 基于EM
  */
object TestLDA {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LDA").setMaster("local")
    val sc = new SparkContext(conf)

    val vocabSize = 40000

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val RDD = sc.textFile("D:\\222_LDA\\kunyanData\\1.segTrainingSet")

    val docDF = RDD.map(line => {
      val temp = line.split(".shtml")
      val contentArr = if (temp.length > 1) {
        val seg = temp(1).trim.split(",")
        seg.map(line => line.replaceAll("[^(a-zA-Z\\u4e00-\\u9fa5)]", "")) //继续过滤数字等无用字符
        seg
      } else {
        Array("开始")
      }
      contentArr
    }).zipWithIndex.toDF("text", "docId")

    val cvModel = new CountVectorizer()
      .setInputCol("text")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(docDF)

    val countVectors = cvModel.transform(docDF)
      .select("docId", "features")
      .map { case Row(docId: Long, countVector: Vector) => (docId, countVector) }
      .cache()


    //模型训练
    val startTime = System.nanoTime()
    val k = 9
    val ldaModel = new LDA().
      setK(k).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(20).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(countVectors)
      .asInstanceOf[DistributedLDAModel]


    //结果输出
    val elapsed = (System.nanoTime() - startTime) / 1e9
    println("[模型训练时间] "+elapsed+"\n===========")
    UtilLDA.printTopWordsPerTopics(ldaModel, cvModel.vocabulary, k)


    //计算任意两个主题之间的相似度
    val averageSim = UtilLDA.calculateAverageSimilar(ldaModel, k)
    println("模型的平均相似度为： "+averageSim)

    //测试最佳K
    val modelArgs = Array(
      9,    //K
      200,  //maxIteration
      5,    //DocConcentration(5)
      5,    //TopicConcentration(5)
      0     //Seed(0L)

    )
//    val bestk=UtilLDA.findBestK(sc,countVectors,modelArgs,4,12,1)
//    println("最佳K： "+bestk._1+ "  :"+ bestk._2)


  }
}
