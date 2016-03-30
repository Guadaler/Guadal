import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlpsuit.sentiment.{TextPre_KunAnalyzer, TextPre_ansj}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by zx on 2016/3/24.
  */
class Test_textPre extends  FlatSpec with Matchers {
  "test " should "work" in {
     val conf = new SparkConf().setAppName("mltest").setMaster("local")
     val sc = new SparkContext(conf)

     //方法测试
//     val dataPath="E:\\data_test\\data2"
//     val outPath="E:\\data_test\\textSeg.txt"

     //本地数据集
     val dataPath="E:\\data\\data"

     val outPath_title="E:\\data\\textSeg_title.txt"
     val outPath_content="E:\\data\\textSeg_content.txt"

     //本地数据集 200
//     val dataPath="E:\\data_200\\data"
//     val outPath="E:\\data_200\\textSeg.txt"

     val stopWordsPath="E:\\data\\stop_words_CN"

     val begin = new Date().getTime
     var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     println("【开始时间】 "+dateFormat.format(begin))

     TextPre_KunAnalyzer.textPre_all(sc,dataPath,outPath_title,outPath_content,stopWordsPath)
//     TextPre_KunAnalyzer.textPre(sc,dataPath,outPath,stopWordsPath)
//     TextPre_ansj.textPre(sc,dataPath,outPath,stopWordsPath)
//     TextPre_ansj.textPre_title(sc,dataPath,outPath_title,stopWordsPath)
//     TextPre_ansj.textPre_content(sc,dataPath,outPath_content,stopWordsPath)

     var end=new Date().getTime
     println("【结束时间】 "+dateFormat.format(end)+"   【耗时】 "+(end-begin))
  }


}
