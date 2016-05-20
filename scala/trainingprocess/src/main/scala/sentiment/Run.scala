package sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlpsuit.util.KunyanConf
import org.apache.spark.{SparkConf, SparkContext}
import sentiment.Util.ParseJson

/**
  * Created by zhangxin on 2016/5/13.
  * 数据[预处理]主程序+模型的[训练]主程序均在此
  */
object Run {

  def main(args: Array[String]) {

    //文本预处理
    textPreMain()

    //模型训练
    trainMain()

  }

  /**
    * 文本预处理主程序
    */
  def textPreMain() {
    //开始时间
    val begin = new Date().getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("【开始时间】 "+dateFormat.format(begin))

    //spark
    val conf = new SparkConf().setAppName("mltest").setMaster("local")
    val sc = new SparkContext(conf)

    val filepath_8 = "D:\\111_DATA\\data\\8【第八次标注】\\result（合并）"
    val stopWordsPath = "D:\\111_DATA\\data\\stop_words_CN"


    //基于Ansj分词器进行预处理（分词，去停）
    val userDics = Array(
      "E:\\dict\\senti_dict\\user_dic.txt",
      "E:\\dict\\senti_dict\\neg_dic.txt",
      "E:\\dict\\senti_dict\\nega_dic.txt",
      "E:\\dict\\senti_dict\\posi_dic.txt"
    )
    val outpath_8 = "D:\\000_DATA\\out\\8【第八次】\\QQ_zx_3400.txt"
    TextPretreat.pretreatWithAnsj(sc, filepath_8, outpath_8, stopWordsPath, userDics)

    //基于坤雁分词器进行预处理
    val configInfo = new ParseJson("D:\\111_DATA\\data\\config.json")

    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    val outPath_8_ky = "D:\\000_DATA\\out\\8【第八次】\\QQ_zx_3400_ky.txt"
    TextPretreat.pretreatWithKunAnal(sc, filepath_8, outPath_8_ky, stopWordsPath,kunyanConfig)


    //结束时间
    val end = new Date().getTime
    println("[结束时间] "+dateFormat.format(end)+"   [耗时] "+(end-begin))

  }

  /**
    * 模型训练主程序
    */
  def trainMain() {
    val sconf=new SparkConf().setAppName("training").setMaster("local")
    val sc=new SparkContext(sconf)
    val filepath="D:\\000_DATA\\out\\8【第八次】\\QQ_zx_3400_ky.txt"
    val modelout="D:\\000_DATA\\Model\\8【第八次标注】"
    TrainWithNb.nbTrainToLocal(sc,filepath,modelout,"model_qq3900",0,2000)
  }

}
