import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlpsuit.sentiment.{TextPre_ansj, TextPre_KunAnalyzer, IO}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by zx on 2016/3/24.
  */
class Test_textPre extends  FlatSpec with Matchers {
  "test " should "work" in {
     val conf = new SparkConf().setAppName("mltest").setMaster("local")
     val sc = new SparkContext(conf)

     //方法测试
     val dataPath="E:\\data_test\\data2"
     val outPath="E:\\data_test\\textSeg.txt"

     //本地数据集
//     val dataPath="E:\\data\\data"
//     val outPath="E:\\data\\textSeg.txt"

     val stopWordsPath="E:\\data\\stop_words_CN"

     val begin = new Date()
     var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     println("【开始时间】 "+dateFormat.format(begin))

//     TextPre_KunAnalyzer.textPre(sc,dataPath,outPath,stopWordsPath)
     TextPre_ansj.textPre(sc,dataPath,outPath,stopWordsPath)

     var end=new Date()
     println("【结束时间】 "+dateFormat.format(end))
  }


}
