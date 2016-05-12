package sentiment

import java.io.{File, PrintWriter}
import java.util.Date

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.SparkContext

/**
  * Created by zhangxin on 2016/3/25.
  * 用坤雁分词器对文本进行预处理: [分词+去停+格式化] 成贝叶斯的训练集格式
  */
object TextpreWithKunAnal {

  /**
    *文本预处理
    *
    * @param sc spark入口
    * @param dataPath 文本路径
    * @param outPath 输出路径
    * @param stopWordsPath 停用词路径
    * @author zhangxin
    */
  def textContentPre(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit = {

    // 获取分词器配置文件信息
    val configInfo = new SentimentConf()
    configInfo.initConfig("D:\\111_DATA\\data\\config.json")

    // 配置坤雁分词
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    //获取停用词
    val stopWords = sc.textFile(stopWordsPath).collect()

    //将处理结果写出到outPath
    val writer = new PrintWriter(new File(outPath), "UTF-8")

    //读取所有文章
    val files = Util.readFile2HashMap(dataPath)
    val it=files.keySet.iterator

    //计数
    var count = 0

    while(it.hasNext){

      //开始时间
      val begin = new Date().getTime

      //取出单篇文章
      val file = it.next()

      //获取单篇文章title和content
      val title = file.getName.substring(0,file.getName.indexOf(".txt"))
      val content = Util.replaceIllegal(files.get(file).toString)

      //计数，便于查问题
      print("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      val content_seg = TextPreprocessing.process(content,stopWords,kunyanConfig)
      val contentResult = content_seg.mkString(",")

      //获取文章类别编号，并写出
      val label = file.getParentFile.getName

      label match {
        case "neg" => writer.append("1#"+contentResult+"\n").flush()
        case "neu" => writer.append("4#"+contentResult+"\n").flush()
        case "pos" => writer.append("4#"+contentResult+"\n").flush()
      }

      //计数
      count += 1

      //结束时间
      val end = new Date().getTime
      println("    【耗时】 "+(end-begin))

    }
    writer.close()
  }

}
