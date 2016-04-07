import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlpsuit.sentiment.{Util, TextPre_KunAnalyzer, TextPre_ansj}
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
//     val dataPath="E:\\data_test\\data"
//     val outPath="E:\\data_test\\textSeg.txt"

     //本地数据集
     val dataPath="D:\\111_DATA\\data\\【第五次标注】tagging"

     val outPath_title="D:\\000_DATA\\out\\【第五次标注程序结果】\\textSeg_title.txt"
     val outPath_content="D:\\000_DATA\\out\\【第五次标注程序结果】\\【1700+1500+1700】textSeg_content.txt"
     val outPath_content_F="D:\\000_DATA\\out\\【第五次标注程序结果】\\F_1700_textSeg_content.txt"
     val outPath_content_S="D:\\000_DATA\\out\\【第五次标注程序结果】\\S_1600_textSeg_content.txt"

     //本地数据集 200
//     val dataPath="E:\\data_200\\data"
//     val outPath="E:\\data_200\\textSeg.txt"

     val stopWordsPath="D:\\111_DATA\\data\\stop_words_CN"

     val begin = new Date().getTime
     var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     println("【开始时间】 "+dateFormat.format(begin))

//     TextPre_KunAnalyzer.textPre_all(sc,dataPath,outPath_title,outPath_content,stopWordsPath)
//     TextPre_KunAnalyzer.textPre(sc,dataPath,outPath,stopWordsPath)
//     TextPre_ansj.textPre(sc,dataPath,outPath,stopWordsPath)
//     TextPre_ansj.textPre_title(sc,dataPath,outPath_title,stopWordsPath)
//     TextPre_ansj.textPre_content(sc,dataPath,outPath_content,stopWordsPath)  //直接三分类
     TextPre_ansj.textPre_content_FS(sc,dataPath,outPath_content_F,outPath_content_S,stopWordsPath)  //二级两分类

    //分完词进一步处理
    /*val out_f="D:\\000_DATA\\out\\【第三次标注程序结果】\\[2]【1000+1000】【pro】textSeg_content.txt"  //中性 +综合
    Util.textPro(outPath_content_F,out_f)

    val out_s="D:\\000_DATA\\out\\【第三次标注程序结果】\\[2]【500+500】【pro】textSeg_content.txt"  //中性 +综合
    Util.textPro(outPath_content_S,out_s)*/

    var end=new Date().getTime
    println("【结束时间】 "+dateFormat.format(end)+"   【耗时】 "+(end-begin))


  }


}
