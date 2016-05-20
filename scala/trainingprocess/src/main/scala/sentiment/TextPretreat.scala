package sentiment

import java.io.{File, PrintWriter}
import java.util.Date

import com.kunyandata.nlpsuit.util.{AnsjAnalyzer, TextPreprocessing, KunyanConf}
import org.apache.spark.SparkContext

/**
  * Created by zhangxin on 2016/5/13.
  * 文本预处理，目的是将数据集批量处理成贝叶斯所需训练集格式
  * 提供基于坤雁和基于ansj两种预处理方法
  */
object TextPretreat {

  /**
    * 基于坤雁分词器对文本进行预处理
    * [分词+去停+格式化] 成贝叶斯的训练集格式
    * @param sc spark入口
    * @param dataPath 文本路径
    * @param outPath 输出路径
    * @param stopWordsPath 停用词路径
    * @author zhangxin
    */
  def pretreatWithKunAnal(sc: SparkContext, dataPath: String, outPath: String,
                          stopWordsPath: String, kunyanConfig: KunyanConf): Unit = {

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

      //获取单篇文章
      val file = it.next()

      //获取单篇文章title和content
      val title = file.getName.substring(0,file.getName.indexOf(".txt"))
      val content = Util.replaceIllegal(files.get(file).toString)

      //计数，便于查问题
      print("[还剩下] "+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

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
      println("    [耗时] "+(end-begin))

    }
    writer.close()
  }

  /**
    * 基于ansj对文本进行预处理
    * [分词+去停+格式化] 成贝叶斯的训练集格式
    * @param sc spark程序入口
    * @param dataPath 文件目录的路径
    * @param outPath 写出文件路径
    * @param stopWordsPath 停用词路径
    * @author zhangxin
    */
  def pretreatWithAnsj(sc: SparkContext, dataPath: String, outPath: String,
                       stopWordsPath: String, userDicts: Array[String]): Unit = {

    //ansj分词器初始化，加载用户词典
    AnsjAnalyzer.init(sc, userDicts)

    //读取文件
    val files = Util.readFile2HashMap(dataPath)

    //读取停用词
    val stopWords = sc.textFile(stopWordsPath).collect()

    //计数
    var count = 0

    //new PrintWriter对象
    val writer = new PrintWriter(new File(outPath),"UTF-8")

    //循环处理文件
    val iterator = files.keySet.iterator
    while(iterator.hasNext){

      val file = iterator.next()
      val title = file.getName.substring(0, file.getName.indexOf(".txt"))
      val content = files.get(file).toString

      //计数，便于查问题，title便于知道是哪篇文章
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      //对文本进行预处理 格式化+分词+去停 处理，返回数组
      val contentTempResult = TextPreprocessing.process(content, stopWords)

      //数组 -> 字符串
      val contentResult = contentTempResult.mkString(",")

      //取文章label，并根据label进行格式化写出
      val label = file.getParentFile.getName

      label match {
        case "neg" => writer.append(1+"#"+contentResult+"\n")
        case "neu" => writer.append(4+"#"+contentResult+"\n")
        case "pos" => writer.append(4+"#"+contentResult+"\n")
      }

      writer.flush()
      count +=1

    }

    writer.close()
  }

}
