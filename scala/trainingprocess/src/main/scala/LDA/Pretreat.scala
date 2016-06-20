package LDA

import java.io.PrintWriter

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import sentiment.Util

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Set}

/**
  * Created by zhangxin on 2016/5/18.
  *
  * 文档预处理  分词+去停+转成dict+vsm
  */
object Pretreat {

  /**
    * 批量新文档预处理 输出DataFrame
    *
    * @param sc sc
    * @param docArray 批量新文档
    * @param stopWordsPath 停用词路径
    * @param kunyanConfig 坤雁分词器配置对象
    * @return 预处理结果DF
    * @author zhangxin
    */
  private def docArray2DF(sc: SparkContext, docArray: Array[String],
                          stopWordsPath: String, kunyanConfig: KunyanConf): DataFrame = {

    //DataFrame
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val stopwords= sc.textFile(stopWordsPath).collect()
    val textRDD = sc.parallelize(docArray)

    //坤雁处理流程包含了分词和去停
    val segTextRDD = textRDD.map(line => {
      val segTemp = TextPreprocessing.process(line, stopwords, kunyanConfig)
      val seg=segTemp.map(line =>line.toString().replaceAll("[^(a-zA-Z\\u4e00-\\u9fa5)]", "")) //过滤数字等无用字符
      seg
    })

    //RDD 转成 DataFrame
    val docDF = segTextRDD.zipWithIndex.toDF("text", "docId")

    docDF
  }

  /**
    * 处理新文档 用于预测
    * 将新文档转成 [词表 + vsm]，并返回
    * @param sc sc
    * @param docArray 批量新文档
    * @param stopWordsPath 停用词路径
    * @param kunyanConfig 坤雁分词器配置对象
    * @return CountVectorizer和VSM模型
    * @author zhangxin
    */
  def doc2VectorsPredict(sc: SparkContext, docArray: Array[String], stopWordsPath: String,
                     kunyanConfig: KunyanConf, vocab:Array[String]) = {

    val docDF= docArray2DF(sc, docArray, stopWordsPath, kunyanConfig)

    //构建CountVectorizer实例
    val cvModel = new CountVectorizer()
      .setInputCol("text")
      .setOutputCol("features")
      .fit(docDF)

    //获取新词表
    val newVocab=cvModel.vocabulary.zipWithIndex.toMap

    //将文本转成向量
    val countVectors = cvModel.transform(docDF)
      .select("docId", "features")
      .map (line =>{

        //预测的新文档需要根据“旧词典”来转
        val docIndex = line.getAs[Long](0)
        val content = line.getAs[SparseVector](1)

        val wordIDs = content.indices  //关键词编号数组，也相当于该文章的词表
        val wordFrequences = content.values     //关键词对应的词频数组，下标对应编号

        val newWordIDs =new Array[Int](vocab.size)
        val newWordFrequences =new Array[Double](vocab.size)

        for(n <- Range(0, vocab.size)){

          newWordIDs(n) = n
          val word = vocab(n)

          //如果该词存在于新的总词表，且该词存在于该文的词表
          if(newVocab.keySet.contains(word)){

            val idInNewVocab = newVocab(word)

            if(wordIDs.contains(idInNewVocab)){
              val frequence = wordFrequences(idInNewVocab)  //取出该词的词频
              newWordFrequences(n) = frequence
            }else{
              newWordFrequences(n) = 0
            }

          }else{
            newWordFrequences(n) = 0
          }

        }

        val content3:Vector=new SparseVector(vocab.size, newWordIDs, newWordFrequences)

        (docIndex,content3)
      }).cache()

    (cvModel,countVectors)
  }

  /**
    * 处理训练文档 用于训练
    * 将新文档转成 [词表 + vsm]，并返回
    * @param sc sc
    * @param docArray 批量新文档
    * @param stopWordsPath 停用词路径
    * @param kunyanConfig 坤雁分词器配置对象
    * @param vocabSize 词表长度
    * @return CountVectorizer和VSM模型
    * @author zhangxin
    */
  def doc2Vectors(sc: SparkContext, docArray: Array[String], stopWordsPath: String,
                     kunyanConfig: KunyanConf, vocabSize: Int) = {

    val docDF = docArray2DF(sc, docArray, stopWordsPath, kunyanConfig)

    //构建CountVectorizer实例
    val cvModel = new CountVectorizer()
      .setInputCol("text")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(docDF)

    //将文档转换为词频标记的向量
    val countVectors = cvModel.transform(docDF)
      .select("docId", "features")
      .map { case Row(docId: Long, countVector: Vector) => (docId, countVector) }
      .cache()

    (cvModel, countVectors)
  }


  //*****************************************
  //*****************************************
  //直接将文本预处理后转成VSM保存到本地

  /**
    * 基于TF_IDf阈值的单词提取
    * @param inFile 文件路径
    * @param wordsDic 词典路径
    * @param sc sc
    * @param minTFIDF TFIDF阈值
    * @return 词典Set集合，无重复项
    * @author zhangxin
    */
  private def getVocabs(inFile: String, wordsDic: String, sc: SparkContext, minTFIDF: Double): Set[String] = {

    val wr = new PrintWriter(wordsDic, "utf-8")

    //词典表
    val words = Set[String]()

    val data = sc.textFile(inFile).map(line => {
      val temp = line.split(".shtml")
      val articleMap = if (temp.length > 1) {
        val content = temp(1).trim
        Util.countWord(content.split(","))
      } else {
        mutable.HashMap("开始"->0)
      }

      articleMap
    }).collect()

    println("[数据读取OVER！！！！！！！！！！]")

    //提取重要的生词，并将生词加入到words
    data.foreach(line => {
      line.foreach(word =>{

        //过滤关键词中除英文和数字之外的所有字符（包括数字）
        val wordRemove = word._1.toString().replaceAll("[^(a-zA-Z\\u4e00-\\u9fa5)]", "")

        //将通过过滤且大于TF_IDf阈值的关键词加入到词表
        if(wordRemove.length == word._1.length){
          val tf_idf = Util.getTf_Idf(word._1, line, data)
          if(tf_idf >= minTFIDF) words += word._1
        }

      })
    })

    //遍历写出
    words.foreach(word => wr.append(word + "\n").flush())

    words
  }

  /**
    * 全单词提取
    * @param inFile 文件路径
    * @param wordsDic 词典路径
    * @param sc sc
    * @return 词典Set集合，无重复项
    * @author zhangxin
    */
  private def getVocabs2(inFile: String, wordsDic: String, sc: SparkContext): Set[String] = {

    val wr=new PrintWriter(wordsDic,"utf-8")

    //词典表
    val words=Set[String]()

    val data = sc.textFile(inFile).map(line => {

      val temp = line.split(".shtml")

      val articleMap = if (temp.length > 1) {
        val content = temp(1).trim
        Util.countWord(content.split(","))
      } else {
        mutable.HashMap("开始"->0)
      }

      articleMap
    }).collect()

    println("[数据读取OVER！！！！！！！！！！]")

    //提取重要的生词，并将生词加入到words
    data.foreach(line => {

      line.foreach(word=> {

        //继续对关键词进行过滤  过滤掉除英文和数字之外的所有字符（包括数字）
        val wordRemove = word._1.toString().replaceAll("[^(a-zA-Z\\u4e00-\\u9fa5)]", "")

        //对过滤通过的关键词加入到词表
        if(wordRemove.length == word._1.length){
          words += word._1
        }

      })
    })

    //返回词表
    words.foreach(word => wr.append(word + "\n").flush())

    words
  }

  /**
    * 文本转成VSM模型
    * @param inFile 文本路径
    * @param wordsDic 词典路径
    * @param sc sc
    * @author  zhangxin
    */
  def text2VSM(inFile: String, wordsDic: String, sc: SparkContext) = {

    val words = getVocabs(inFile, wordsDic, sc, 0.3)

//    val words = Source.fromFile(wordsDic).getLines().toArray

    val file2 = sc.textFile(inFile).map(line => {

      val temp = line.split(".shtml")

      val contentArr = if (temp.length > 1) {
        temp(1).trim.split(",")
      } else {
        Array("开始")
      }

      contentArr
    })

    //得到每篇文章的词频map
    val data = file2.map(line => {
      Util.countWord(line)
    }).collect()

    // 对每篇文章，以词典表进行遍历
    // Array[(Long,Vector)]
    val vsmTemp = data.map(line=>{

      val lineVectorArray = ArrayBuffer[Double](words.size)

      words.foreach(word => {

        var n = 0.0D
        if(line.keySet.contains(word)) {n = line(word).toDouble}
        lineVectorArray += n

      })

      Vectors.dense(lineVectorArray.toArray)
    })
      .zipWithIndex.map(_.swap)
      .map(v=>(v._1.toLong, v._2))

    // RDD[(Long,Vector)]
    (words,sc.parallelize(vsmTemp))
  }

}