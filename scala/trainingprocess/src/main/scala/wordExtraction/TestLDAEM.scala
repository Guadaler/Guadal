package wordExtraction

import LDA.UtilLDA
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangxin on 2016/5/18.
  *
  * LDA模型训练+本地模式
  */
object TestLDAEM {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("LDATraining").setMaster("local")
    val sc=new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //1、数据加载
    val RDD =sc.textFile("D:\\333_WordExtraction\\Run_hancks\\Data\\4.datahankcs_vsm_feature.txt")
    val parsedData = RDD.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))

    // 为文档编号，编号唯一
    //.zipWithIndex  将元素和其所在的下表组成一个pair
    //.swap 将两个变量数值的交换
    val countVectors = parsedData.zipWithIndex.map(_.swap).cache()

//    val RDD =sc.textFile("D:\\333_WordExtraction\\Run\\Data\\2.segTrainingSet")
//
//    val docDF=RDD.map(line=> {
//      val temp = line.split(".shtml")
//      val contentArr = if (temp.length > 1) {
//        val seg=temp(1).trim.split(",")
//        seg.map(line =>line.replaceAll("[^(a-zA-Z\\u4e00-\\u9fa5)]", "")) //继续过滤数字等无用字符
//        seg
//      } else {
//        Array("开始")
//      }
//      contentArr
//    }).zipWithIndex.toDF("text", "docId")
//
//    val cvModel = new CountVectorizer()
//      .setInputCol("text")
//      .setOutputCol("features")
//      .setVocabSize(5000)
//      .fit(docDF)
//
//    val countVectors = cvModel.transform(docDF)
//      .select("docId", "features")
//      .map { case Row(docId: Long, countVector: Vector) => (docId, countVector) }
//      .cache()

    val modelArgs=Array(
      6,    //K
      60,  //maxIteration
      5,    //DocConcentration(5)
      5,    //TopicConcentration(5)
      0     //Seed(0L)
    )

//    val bestK=UtilLDA.findBestK(sc,countVectors,modelArgs,4,12,1)
//    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<最佳K： "+bestK._1+ "  : " +bestK._2)

    //模型训练
    val startTime = System.nanoTime()
    val k=9
    val ldaModel=new LDA().
      setK(k).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(20).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(countVectors)
//      .asInstanceOf[DistributedLDAModel]

    //模型保存
//    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<[模型保存ing]")
//    val modelPath="D:\\333_WordExtraction\\Run_hancks\\Model"
//    UtilLDA.deleteModel(modelPath)
//    ldaModel.save(sc,modelPath)
//    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<[模型保存结束]")


    //加载词表
    val vocab=sc.textFile("D:\\333_WordExtraction\\Run_hancks\\Dict\\vocab.txt").collect()

    //3 模型输出，模型参数输出，结果输出
    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 1)) {
      print("Topic " + topic + ":")
      var sum=0.0
      for (word <- Range(0, ldaModel.vocabSize)) { println(" " + topics(word, topic)); }
      for (word <- Range(0, ldaModel.vocabSize)) {
        sum =sum+topics(word, topic)
      }
      println("#####sum : "+sum)
      println()
    }


    //主题——词分布
    println("<<<<<<<<<<<<<<<主题-词分布>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println("model.vocabSize:"+ldaModel.vocabSize)
    val phi=ldaModel.topicsMatrix
    for (topic <- Range(0, ldaModel.k)) {
      println("Topic " + topic + "  概率:")
      var sum=0.0
      for (word <- Range(0, 20)) {
        println("    " +vocab(word)+" : "+ phi(word, topic))
      }

      for (word <- Range(0, vocab.size)) {
        sum =sum+phi(word, topic)
      }
      println(sum)
    }

    //结果输出
    val elapsed = (System.nanoTime() - startTime) / 1e9
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<[模型训练时间] "+elapsed+"\n===========")
//    UtilLDA.printTopWordsPerTopics(ldaModel,cvModel.vocabulary,k)
    UtilLDA.printTopWordsPerTopics(ldaModel,sc.textFile("D:\\333_WordExtraction\\Run_hancks\\Dict\\vocab.txt").collect(),k)

//
//    //词汇量
//    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<词汇量：  "+cvModel.vocabulary.size)
//    val vocab=cvModel.vocabulary
//    val wr=new PrintWriter("D:\\333_WordExtraction\\Run\\Dict\\vocab.txt","utf-8")
//    vocab.foreach(word => wr.append(word+"\n").flush())
//    wr.close()

    //计算任意两个主题之间的相似度
    val averageSim=UtilLDA.calculateAverageSimilar(ldaModel,k)
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<模型的平均相似度为： "+averageSim)
  }
}
