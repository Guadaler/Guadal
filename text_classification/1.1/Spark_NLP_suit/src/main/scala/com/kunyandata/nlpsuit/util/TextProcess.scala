package com.kunyandata.nlpsuit.util

import java.io.{PrintWriter, BufferedWriter, File, FileWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * Created by QQ on 2016/3/18.
  *
  */

import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer

object TextProcess {

  /**
    * 格式化文本，转化空白字符为停用词表中的标点符号，同时统一英文字母为小写
    *
    * @param content 原始文本字符串
    * @return 返回格式化之后的字符串
    */
  def formatText(content: String): String = {

    val step = 65248
    val dbcStart = 33.toChar
    val dbcEnd = 126.toChar
    val sbcStart = 65281.toChar
    val sbcEnd = 65374.toChar
    val sbcSpace = 12288.toChar
    val dbcSpace = 32.toChar
    val bufferString = new ArrayBuffer[Char]
    if(content == null){
      content
    }else{
      content.foreach(ch => {
        if (ch == sbcSpace){
          bufferString.append(dbcSpace)
        }else if (ch >= sbcStart && ch <= sbcEnd){
          bufferString.append((ch - step).toChar)
        }else{
          bufferString.append(ch)
        }
      })
      bufferString.mkString.replaceAll("[ \n\b\r\f\t]", "")
    }
  }

  /**
    * 去除分词结果中的标点符号和停用词
    *
    * @param content 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWords(content: Seq[String], stopWords:Seq[String]): Seq[String] = {
    var result = content.toBuffer
    stopWords.foreach(stopWord => {
      if (result.contains(stopWord)){
        result = result.filterNot(_ == stopWord)
      }
    })
    result.toSeq
  }

  /**
    * 调用WordSeq里面的函数实现字符串的分词和去停,并分装成方法
    *
    * @param content 需要处理的字符串
    * @param stopWordsBr 停用词
    * @return 返回分词去停后的结果
    */
  def process(content: String, stopWordsBr: Broadcast[Seq[String]]): Seq[String] = {
    // 格式化文本
    val formatedContent = formatText(content)
    // 实现分词
    val splitWords = WordSeg.splitWord(formatedContent, 0)
    // 读取分词内容并转化成Array格式
    val resultWords = WordSeg.getWords(splitWords)
    // 实现去停用词
    if (resultWords == null) null
    else removeStopWords(resultWords, stopWordsBr.value)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordSegmentation")
    val sc = new SparkContext(conf)
    val stopWords = sc.textFile("hdfs://222.73.34.92:9000/mlearning/dicts/stop_words_CN").collect().toSeq
    //    val stopWords = Source.fromFile("/home/mlearning/dicts/stop_words_CN").getLines().toSeq
    //    val stopWords = Source.fromFile("D:/mlearning/dicts/stop_words_CN").getLines().toArray
    val stopWordsBr = sc.broadcast(stopWords)
    //    val trainingSet = "，，财政部公布的1万亿地方债务置换计划加上央行将地方债纳入抵押品范围这一配套鼓励措施,这些料将缓减市场对于地方政府融资平台贷款的担忧。，，，鉴于2014年盈利略好于我们的预期以及央行5月份降息对银行的影响,我们对2015-16年的盈利预测进行了微调。同时,我们引入了2017年的盈利预测,预期我们所覆盖的港股中资银行2015-17年的净利润增速同比将达到2.7%/4.5%/8.8%。-，l,，W,，7，K&amp;，w5，X&amp;，i，，，维持对工商银行（601398）、建设银行（601939）、农业银行（601288）、中国银行（601988）、交通银行和重庆农商行的买入评级;维持对民生和中信银行（601998）的卖出评级;维持对盛京银行的中性评级。首推中国银行和交通银行;重申对整个板块的推荐评级。，，，货币政策放松加之财政政策支持料将有助于消减资产质量压力。;，E.，/，H\"，V!，y-，B.，a_，，，我们预期今年年底之前央行将至少再度降息25个基点,这应该会进一步缓减中资银行所面临的资产质量压力。此外,财政部公布了1万亿元地方政府债务置换计划以降低地方政府今年的还本付息压力,这应该会使地方政府平台贷款的资产质量暂时无忧。尽管我们预期2015-17年随着宏观经济的持续放缓银行的不良贷款仍将上升,但我们相信这些有利的货币和财政政策应该会防止银行的资产质量出现全面性的爆发。，，，对2015-16年盈利进行微调,并首次引入2017年盈利预测。*，T3，{)，~5，d\"，g，，，鉴于2014年的盈利数据略好于我们的预期以及5月份央行降息对中资银行的盈利影响,我们对整个板块2015和2016年的盈利预测分别上调了1.1%和下调了0.6%。同时,我们首次引入了对2017年的盈利预测,并且预期港股中资银行2015/16/17年的盈利增速将同比达到2.7%/4.5%/8.8%。9，S/，g8，T!，m7，d&amp;，y，l9，M!，o6，Z#，k，，，催化剂及估值。，，，货币政策进一步放松以及可能出台的有利财政政策(例如扩大地方政府债务置换规模等)将会成为推动股价上涨的催化剂。.，L;，G&amp;，F.，b2，l9，P，，，由于我们下调了对长期成本收入比的假设并上调了完成利率市场化之后长期的非息收入对营业收入的贡献比例,我们将港股中资银行的长期股本回报率由此前的平均11.28%上调至12.62%,从而将公允目标价由此前的0.58x-1.10x上调至0.65x-1.26x。!，x9，E;，O,，x&amp;，T，，，目前整个板块的估值水平为1.03倍的15年市净率以及6.62倍的15年市盈率,我们认为相对于1.26倍的历史平均市净率和7.17倍的历史平均市盈率,目前的估值水平已经具有吸引力。我们对整个板块维持谨慎乐观的看法,并维持推荐评级。首推中国银行和交通银行。"
    //    println(formatText(trainingSet))
    //    println(WordSeg.splitWord(formatText(trainingSet), 0))
    //    println(process(trainingSet, stopWordsBr))

    // 驱动名称
    //    val driver = "com.mysql.jdbc.Driver"
    //    //访问本地mysql服务器,通过3306端口
    //    // val url = "jdbc:mysql://localhost/mysql"
    //    // val url = "jdbc:mysql://localhost:3306/mysql"
    //    // val url = "jdbc:mysql://127.0.0.1:3306/mysql"
    //    // 访问其他服务器
    //    val jdbcUrl = "jdbc:mysql://192.168.1.14:3306/mysql"
    //    // 用户名
    //    val username = "root"
    //    // 密码
    //    val password = "root"
    //    // 读取sql里面的内容
    //    // val sqlString="SELECT * FROM indus_text_with_label WHERE id>3000 and id<=15000"
    //    val sqlString = "SELECT url, content FROM stock.indus_text_with_label group by url"
    //    // 调用函数获取数据库中的数据
    //    val connection = MySQLUtil.getConnect(driver, jdbcUrl, username, password)
    //    val result = MySQLUtil.getResult(connection,sqlString)

    //     解析数据库中的数据
    //        var results = new ArrayBuffer[Tuple4[String,String,String,String]]
    //        while ( result.next()) {
    //          val id = result.getString("id").trim
    //          val indus_code = result.getString("indus_code").trim
    //          val title = result.getString("title").trim
    //          val content = result.getString("content").trim
    //          results.append((id, indus_code, title, content, url)
    //        }

    //     定义splitResults 保存分词结果
    //    val splitResults = new ArrayBuffer[(String, Array[String])]
    //    while (result.next()) {
    //      val url = result.getString("url").trim
    //      val content = result.getString("content").trim.replaceAll("[ 　\f\n\r\b\t]", "，")
    //      val segContent = TextProcess.process(content, stopWordsBr)
    // println(content)
    // println(wordsList.toSeq)
    //      splitResults.append((url, content))
    //    }
    //     分词
    //    val trainingSet = Source.fromFile("/home/mlearning/trainingData/TrainingSet").getLines().toArray
    //    val trainingSet =sc.parallelize(Source.fromFile("D:/mlearning/trainingSet").getLines().toSeq)
        val trainingSet = sc.textFile("hdfs://222.73.34.92:9000/mlearning/trainingSet")
    //    val hdfsConf = new Configuration()
    //    hdfsConf.set("fs.defaultFS", "hdfs://222.73.34.92:9000")
    //    val fs = FileSystem.get(hdfsConf)
    //    val output = fs.create(new Path("/mlearning/segTrainSet"))
    //    val writer = new PrintWriter(output)

    //    val DataFile = new File("D:/mlearning/segTrainingSet")
    //    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
        trainingSet.map(line => {
          val temp = line.split("\t")
          if (temp.length == 2){
            val segResult = TextProcess.process(temp(1), stopWordsBr)
            if (segResult != null) {
              println((temp(0), segResult))
              temp(0) + "\t" + segResult.mkString(",") + "\n"
            }
          }
        }).saveAsTextFile("hdfs://222.73.34.92:9000/mlearning/segTrainSet")

    // wordseg test
//    println(WordSeg.splitWord("给我一个理由忘记，那么爱我的你！", 0))
    sc.stop()
  }
}