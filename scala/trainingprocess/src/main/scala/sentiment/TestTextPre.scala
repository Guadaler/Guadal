package sentiment

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2016/4/21.
  */
object TestTextPre {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("mltest").setMaster("local")
    val sc = new SparkContext(conf)

   /* val data=sc.textFile("D:\\111_DATA\\text4.txt")
    var datamap=data.map(element =>{
      println(element)
    }).collect().foreach(println(_))*/

    val stopWordsPath="D:\\111_DATA\\data\\stop_words_CN"
    val begin = new Date().getTime
    var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("【开始时间】 "+dateFormat.format(begin))

    val negfile="D:\\111_DATA\\data\\【第六次标注】tagging（QQ_3900）\\【deal_removeTimeTitle】新闻样本 - 副本.txt"
    val posfile="D:\\111_DATA\\data\\【第六次标注】tagging（QQ_3900）\\pos_neu"
    val outpath="D:\\000_DATA\\out\\【第六次标注程序结果】\\QQ_3900.txt"


    val filepath="D:\\111_DATA\\data\\【第七次标注】tagging（QQ_3300）"
    val outpath2="D:\\000_DATA\\out\\【第七次标注程序结果】\\QQ_3300.txt"

    val filepath_8="D:\\111_DATA\\data\\8【第八次标注】\\result（合并）"
    val outpath_8="D:\\000_DATA\\out\\8【第八次】\\QQ_zx_3400.txt"  //

    //数据集预处理成一个txt文件（分词，去停）
//    TextpreWithAnsj.textPre(sc,negfile,posfile,outpath,stopWordsPath)
    TextpreWithAnsj.textPre_content_F(sc,filepath_8,outpath_8,stopWordsPath)




    //negfile ->local
    val outp="D:\\111_DATA\\data\\【第六次标注】tagging（QQ_3900）\\neg_title"
//    TextpreWithAnsj.write2local(sc,negfile,outp)


    //
    var end=new Date().getTime
    println("【结束时间】 "+dateFormat.format(end)+"   【耗时】 "+(end-begin))

  }

}
