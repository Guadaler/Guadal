package wordExtraction

import com.kunyandata.nlpsuit.util.KunyanConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by zhangxin on 2016/5/18.
  *
  * 文档预处理
  */
object  Pretreat {

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

      //坤雁分词
//      val segTemp = TextPreprocessing.process(line, stopwords, kunyanConfig)

      //ansj分词（利用词性过滤无意义词）
      val segTemp = TextPreprocessing.process(line, stopwords)

      //hancks文本
      //val segTemp = line.split(" ")

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
}