package wordExtraction

import java.io.PrintWriter
import java.net.URI

import LDA.{LoggerUtil, UtilLDA}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangxin on 2016/5/18.
  *
  * LDA模型训练+Spark集群
  */
object TestLDAEM_Spark {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("LDATraining").setMaster("local")
    val sc=new SparkContext(conf)
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<SC初始化结束")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val RDD =sc.textFile(args(0))

    val docDF=RDD.map(line=> {
      val temp = line.split(".shtml")
      val contentArr = if (temp.length > 1) {
        val segTemp=temp(1).trim.split(",")
        val seg=segTemp.map(line =>line.replaceAll("[^(a-zA-Z\\u4e00-\\u9fa5)]", "")) //继续过滤数字等无用字符
        seg
      } else {
        Array("开始")
      }
      contentArr
    }).zipWithIndex.toDF("text", "docId")

    val cvModel = new CountVectorizer()
      .setInputCol("text")
      .setOutputCol("features")
      .setVocabSize(25000)
      .fit(docDF)

    val countVectors = cvModel.transform(docDF)
      .select("docId", "features")
      .map { case Row(docId: Long, countVector: Vector) => (docId, countVector) }
      .cache()

    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<数据集加载结束")

    val modelArgs=Array(
      6,    //K
      60,  //maxIteration
      5,    //DocConcentration(5)
      5,    //TopicConcentration(5)
      0     //Seed(0L)

    )

    //获取最佳K
    val bestK=UtilLDA.findBestK(sc,countVectors,modelArgs,4,12,1)
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<最佳K： "+bestK._1+ "  : " +bestK._2)

    //模型训练
    val startTime = System.nanoTime()
    val k=args(1).toInt
    val maxIter=args(2).toInt
    val ldaModel=new LDA().
      setK(k).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(maxIter).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(countVectors)
      .asInstanceOf[DistributedLDAModel]


    //结果输出
    val elapsed = (System.nanoTime() - startTime) / 1e9
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<[模型训练时间] "+elapsed+"\n===========")
    UtilLDA.printTopWordsPerTopics(ldaModel,cvModel.vocabulary,k)

    //模型保存
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<[模型保存ing]")

    val conf2 = new Configuration()
    val uri="hdfs://222.73.57.12:9000"
    val fs=FileSystem.get(URI.create(uri),conf2)
    val p=new Path("/user/zhangxin/RunTrain/")
    val a=fs.delete(p,true)
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<删除结果： "+a)

    val modelPath="hdfs://222.73.57.12:9000/user/zhangxin/RunTrain/"
    ldaModel.save(sc,modelPath)
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<[模型保存结束]")


    //词汇量
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<词汇量："+cvModel.vocabulary.size)
    val vocab=cvModel.vocabulary
    val wr=new PrintWriter(args(3))
    vocab.foreach(word => wr.append(word+"\n").flush())
    wr.close()

    //计算任意两个主题之间的相似度
    val averageSim=UtilLDA.calculateAverageSimilar(ldaModel,k)
    LoggerUtil.warn("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<模型的平均相似度为： "+averageSim)
  }
}
