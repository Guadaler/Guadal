import java.io.{File, PrintWriter}

import com.kunyandata.nlpsuit.sentiment.IO
import com.kunyandata.nlpsuit.util.{TextProcess, WordSeg}
import org.apache.spark.{SparkContext, SparkConf}
import org.mortbay.util.ajax.JSON
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by zx on 2016/3/24.
  */
class Test_textSeg extends  FlatSpec with Matchers {
  "test " should "work" in {
     val conf = new SparkConf().setAppName("mltest").setMaster("local")
     val sc = new SparkContext(conf)
     val dataPath="E:\\data_test\\data"
     val outPath="E:\\data_test\\textSeg.txt"
     val stopWords = sc.textFile("E:\\data_test\\stop_words_CN").collect()

    var writer=new PrintWriter(new File(outPath),"UTF-8")

//     val files=IO.readfile(dataPath)
////     val it=files.keySet.iterator
//     for(file <-files.keys){
//       val title=file.getName;
//       val content=files.get(file).toString
//       val title_segJson=WordSeg.splitWord(title,1)
//       val content_segJson=WordSeg.splitWord(content,1)
//
//       // Json ->Array
//       var title_seg=WordSeg.getWords(title_segJson)
//       var content_seg=WordSeg.getWords(content_segJson)
//
//       //去停
//       title_seg=TextProcess.removeStopWords(title_seg,stopWords)
//       content_seg=TextProcess.removeStopWords(content_seg,stopWords)
//
//
//       val label=file.getParentFile.getName
//       val writer=IO.writefile_append(outPath,title_seg+" "+content_seg+"\n")
//     }

    val files=IO.readfile2(dataPath)
    val it=files.keySet.iterator
    while(it.hasNext){
      val file=it.next()
      val title=file.getName
      val content=files.get(file).toString
      val title_segJson=WordSeg.splitWord(TextProcess.formatText(title),1)
      val content_segJson=WordSeg.splitWord(TextProcess.formatText(content),1)
//      println(file.getAbsolutePath)
//      println(content_segJson)

      // Json ->Array
      var title_seg=WordSeg.getWords(title_segJson)
      var content_seg=WordSeg.getWords(content_segJson)

      //去停
      title_seg=TextProcess.removeStopWords(title_seg,stopWords).
      content_seg=TextProcess.removeStopWords(content_seg,stopWords)
      var title=""
      for(word <-title_seg){
        title =title+" "+word.toString()
      }

      val label=file.getParentFile.getName
//      val writer=IO.writefile_append(outPath,title_seg+" "+content_seg+"\n")

      println(title_seg)
      writer.append(title_seg+" "+content_seg+"\n")
      writer.flush()
    }
    writer.close()
  }


}
