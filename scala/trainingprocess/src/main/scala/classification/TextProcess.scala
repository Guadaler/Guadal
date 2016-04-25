package classification

import org.apache.spark.{SparkConf, SparkContext}
import com.kunyan.nlpsuit.util.TextPreprocessing

/**
  * Created by root on 4/19/16.
  */
object TextProcess {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordSegmentation")
    val sc = new SparkContext(conf)
    val segAppPath = "/home/mlearning/bin/"
    val stopWords = sc.textFile("hdfs://222.73.34.92:9000/mlearning/dicts/stop_words_CN").collect()
    //    val stopWords = Source.fromFile("/home/mlearning/dicts/stop_words_CN").getLines().toArray
    //    val stopWords = Source.fromFile("D:/mlearning/dicts/stop_words_CN").getLines().toArray
    val stopWordsBr = sc.broadcast(stopWords)
    //    val trainingSet = "，，财政部公布的1万亿地方债务置换计划加上央行将地方债纳入抵押品范围这一配套鼓励措施,这些料将缓减市场对于地方政府融资平台贷款的担忧。，，，鉴于2014年盈利略好于我们的预期以及央行5月份降息对银行的影响,我们对2015-16年的盈利预测进行了微调。同时,我们引入了2017年的盈利预测,预期我们所覆盖的港股中资银行2015-17年的净利润增速同比将达到2.7%/4.5%/8.8%。-，l,，W,，7，K&amp;，w5，X&amp;，i，，，维持对工商银行（601398）、建设银行（601939）、农业银行（601288）、中国银行（601988）、交通银行和重庆农商行的买入评级;维持对民生和中信银行（601998）的卖出评级;维持对盛京银行的中性评级。首推中国银行和交通银行;重申对整个板块的推荐评级。，，，货币政策放松加之财政政策支持料将有助于消减资产质量压力。;，E.，/，H\"，V!，y-，B.，a_，，，我们预期今年年底之前央行将至少再度降息25个基点,这应该会进一步缓减中资银行所面临的资产质量压力。此外,财政部公布了1万亿元地方政府债务置换计划以降低地方政府今年的还本付息压力,这应该会使地方政府平台贷款的资产质量暂时无忧。尽管我们预期2015-17年随着宏观经济的持续放缓银行的不良贷款仍将上升,但我们相信这些有利的货币和财政政策应该会防止银行的资产质量出现全面性的爆发。，，，对2015-16年盈利进行微调,并首次引入2017年盈利预测。*，T3，{)，~5，d\"，g，，，鉴于2014年的盈利数据略好于我们的预期以及5月份央行降息对中资银行的盈利影响,我们对整个板块2015和2016年的盈利预测分别上调了1.1%和下调了0.6%。同时,我们首次引入了对2017年的盈利预测,并且预期港股中资银行2015/16/17年的盈利增速将同比达到2.7%/4.5%/8.8%。9，S/，g8，T!，m7，d&amp;，y，l9，M!，o6，Z#，k，，，催化剂及估值。，，，货币政策进一步放松以及可能出台的有利财政政策(例如扩大地方政府债务置换规模等)将会成为推动股价上涨的催化剂。.，L;，G&amp;，F.，b2，l9，P，，，由于我们下调了对长期成本收入比的假设并上调了完成利率市场化之后长期的非息收入对营业收入的贡献比例,我们将港股中资银行的长期股本回报率由此前的平均11.28%上调至12.62%,从而将公允目标价由此前的0.58x-1.10x上调至0.65x-1.26x。!，x9，E;，O,，x&amp;，T，，，目前整个板块的估值水平为1.03倍的15年市净率以及6.62倍的15年市盈率,我们认为相对于1.26倍的历史平均市净率和7.17倍的历史平均市盈率,目前的估值水平已经具有吸引力。我们对整个板块维持谨慎乐观的看法,并维持推荐评级。首推中国银行和交通银行。"
    //    println(formatText(trainingSet))
    //    println(WordSeg.splitWord(formatText(trainingSet), 0))
    //    println(process(trainingSet, stopWordsBr))

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
    //    val trainingSet = Source.fromFile("D:/mlearning/trainingSet").getLines().toArray
    // val trainingSet =sc.parallelize(Source.fromFile("D:/mlearning/trainingSet").getLines().toSeq)
    //    val trainingSet = sc.textFile("hdfs://222.73.34.92:9000/mlearning/trainingSet")
    // test data
    val trainingSet = sc.textFile("hdfs://222.73.34.92:9000/mlearning/trainingSet")
    //    val hdfsConf = new Configuration()
    //    hdfsConf.set("fs.defaultFS", "hdfs://222.73.34.92:9000")
    //    val fs = FileSystem.get(hdfsConf)
    //    val output = fs.create(new Path("/mlearning/segTrainSet"))
    //    val writer = new PrintWriter(output)

    //    val DataFile = new File("/home/mlearning/result/segTrainingSet")
    ////    val DataFile = new File("D:/mlearning/segTrainingSet")
    //    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
    //    val fileoutput = new FileInputStream("D:/segApp/dict_20150526_web2.core")
    //    val fileinput = new FileOutputStream("D:/dict_20150526_web2.core")

    //    println("current 1: ===============" + System.getProperty("user.dir"))
    trainingSet.map(line => {
      val temp = line.split("\t")
      //      println("current 2: ===============" + System.getProperty("user.dir"))
      if (temp.length == 2){
        //        val segJson = WordSeg.splitWord(temp(1), segAppPath, 0)
        val segResult = TextPreprocessing.process(temp(1), stopWordsBr.value)
        if (segResult != null) temp(0) + "\t" + segResult.mkString(",")
      }
    }).saveAsTextFile("hdfs://222.73.34.92:9000/mlearning/segResult")
    sc.stop()
  }
}
