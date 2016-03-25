import java.io.{FileInputStream, File, FileReader}

import com.kunyandata.nlpsuit.sentiment.Analyzer
import com.kunyandata.nlpsuit.util.WordSeg
import org.scalatest.{Matchers, FlatSpec}

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by zx on 2016/3/25.
  * 测试某些“有问题”文章导致程序中断
  */
class Test_wordseg extends  FlatSpec with Matchers {
  "test " should "work" in {
    var file="E:\\data_test\\data2\\neg\\煤价坠落 大型煤企探路套保.txt"
    var file2="E:\\data_test\\data2\\pos\\乐森悦：9月18美联储推迟加息 现货原油后市走势分析.txt"
    var file1="E:\\data_test\\data2\\pos\\“互联网+”的下一个风口：农业.txt"
    var str:String="　　- 互联网用户：6.68亿，同比增幅6%\n　　- 社交媒体用户：6.59亿 ， 超过美国和欧洲的总和\n　　- 手机单独用户：6.75亿，手机开户入网用户数量有13亿\n　　- 手机互联网用户：5.94亿， 占中国所有网民的89%\n　　- 手机社交媒体用户： 5.74亿，同比增幅1.5%"
    for(line <-Source.fromFile(file).getLines()){
      str +=line.trim()

      println()
      println(line)

      //用坤雁分词
      //    println(str)
      val result = WordSeg.splitWord(line,1)
      println(result)
      val jsonResult = JSON.parseFull(result)
      val result1 = jsonResult.get.asInstanceOf[Map[String, Any]]
      val a = result1("result")
        .asInstanceOf[Map[String, Any]]("segment")
        .asInstanceOf[List[Map[String, String]]]
      val c = a.map(line => {
        line("word")
      }).toSeq
      println(c)
    }

    //用ansj分词
    /*var ansj_result=Analyzer.cut(str)
    for(word <- ansj_result){
      println(word)
    }*/

   /* //用坤雁分词
//    println(str)
    val result = WordSeg.splitWord(str,1)
    println(result)
    val jsonResult = JSON.parseFull(result)
    val result1 = jsonResult.get.asInstanceOf[Map[String, Any]]
    val a = result1("result")
      .asInstanceOf[Map[String, Any]]("segment")
      .asInstanceOf[List[Map[String, String]]]
    val c = a.map(line => {
      line("word")
    }).toSeq
    println(c)*/

  }
}
