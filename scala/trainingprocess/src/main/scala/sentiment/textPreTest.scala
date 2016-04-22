package sentiment

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zx on 2016/4/21.
  */
object textPreTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("mltest").setMaster("local")
    val sc = new SparkContext(conf)
    val stopWordsPath="D:\\111_DATA\\data\\stop_words_CN"
    val begin = new Date().getTime
    var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("【开始时间】 "+dateFormat.format(begin))

    val negfile="D:\\111_DATA\\data\\【第六次标注】tagging（QQ_3900）\\【deal_removeTimeTitle】新闻样本.txt"
    val posfile="D:\\111_DATA\\data\\【第六次标注】tagging（QQ_3900）\\pos_neu"
    val outpath="D:\\000_DATA\\out\\【第六次标注程序结果】\\QQ_3900.txt"
    TextpreWithAnsj.textPre(sc,negfile,posfile,outpath,stopWordsPath)


    var end=new Date().getTime
    println("【结束时间】 "+dateFormat.format(end)+"   【耗时】 "+(end-begin))


  }
}
