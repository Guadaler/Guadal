package sentiment

import java.io.{File, PrintWriter}

import com.kunyan.nlpsuit.util.{AnsjAnalyzer, TextPreprocessing}
//import com.kunyandata.nlpsuit.sentiment.Analyzer
//import com.kunyandata.nlpsuit.sentiment.TextPre_ansj
//import com.kunyandata.nlpsuit.sentiment.Util
//import com.kunyandata.nlpsuit.util.TextProcess
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zx on 2016/3/25. 用ansj分词器对文本进行预处理
  */
object TextpreWithAnsj {

  /**
    * 初始化
    *
    * @param sc
    */
  def init(sc: SparkContext): Unit = {
    //分词器初始化，加载用户词典
    val user_dict = Array(
      "E:\\dict\\senti_dict\\user_dic.txt",
      "E:\\dict\\senti_dict\\neg_dic.txt",
      "E:\\dict\\senti_dict\\nega_dic.txt",
      "E:\\dict\\senti_dict\\posi_dic.txt"
    )
    AnsjAnalyzer.init(sc, user_dict)
  }

  /**
    * 将分词后得到的分词数组转成字符串
    *
    * @param segArray
    * @return 字符串
    * @author zhangxin
    */
  def array2Str(segArray: Array[String]): String = {
    var contentstr = ""
    for (word <- segArray) {
      contentstr += "," + word.toString()
    }
    contentstr
  }

  def textPre(sc: SparkContext, negfile: String, posPath: String, outPath: String, stopWordsPath: String): Unit = {
    val stopWords = sc.textFile(stopWordsPath).collect()
    val stopwordsbr=sc.broadcast(stopWords)
    TextpreWithAnsj.init(sc)
    var count = 0
    //读取文件
    val files = Util.readFile2HashMap(posPath)

    val negrdd = sc.textFile(negfile)
    val all=negrdd.count()+files.size()

    val segNegArr = negrdd.map(element =>{
      val temp=TextPreprocessing.process(TextPreprocessing.formatText(element),stopwordsbr)
      val tempcontent=array2Str(temp)
      println("还剩下："+ (all - count))
      count +=1
      ("1#"+tempcontent+"\n")
    }).collect()

    val writer = new PrintWriter(new File(outPath), "UTF-8")
    var posArr=ArrayBuffer[String]()

    val it = files.keySet.iterator
    while (it.hasNext) {
      val file = it.next()
      val title = file.getName.substring(0, file.getName.indexOf(".txt"))
      val content = files.get(file).toString

      println("还剩下：" + (files.size() - count) + "  [" + file.getParentFile.getName + "]  " + title)

      val seg=TextPreprocessing.process(TextPreprocessing.formatText(content),stopwordsbr)
      val str = array2Str(seg)
      posArr +="4#"+str+"\n"
      count += 1
    }
    posArr ++=segNegArr
    posArr.toArray.foreach(line =>writer.append(line.toString))
    writer.close()
  }

  /**
    *
    * @param sc
    * @param dataPath
    * @param outPath
    * @param stopWordsPath
    */
  def textPre_content_F(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val stopwordsbr=sc.broadcast(stopWords)
    val writer=new PrintWriter(new File(outPath),"UTF-8")
    //读取文件
    val files=Util.readFile2HashMap(dataPath)
    val it=files.keySet.iterator

    //初始化
    TextpreWithAnsj.init(sc)

    //计数
    var count=0

    while(it.hasNext){

      val file=it.next()
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=files.get(file).toString

      //计数，便于查问题，title便于知道是哪篇文章
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      var content_seg=TextPreprocessing.process(content,stopwordsbr)
      var contentstr=array2Str(content_seg)

      val label=file.getParentFile.getName
      label match {
        case "neg" =>writer.append(1+"#"+contentstr+"\n")
        case "neu" =>writer.append(4+"#"+contentstr+"\n")
        case "pos" =>writer.append(4+"#"+contentstr+"\n")
      }
      writer.flush()

      count +=1
    }
    writer.close()
  }

  def write2local(sc:SparkContext,inpath:String,outpath:String): Unit ={
    var count=0
    val data=sc.textFile(inpath)
    val segNegArr =data.map(line=>{
      val title=line.substring(0,line.indexOf("\t"))
      val content=line.substring(line.indexOf("\t")+1,line.length)
      count +=1
      val writer2=new PrintWriter(outpath+"\\"+Util.replace(title)+".txt","utf-8")
      println("路径为： "+outpath+"\\"+title+".txt")
      writer2.write(TextPreprocessing.formatText(content))
      writer2.close()
    } ).collect()
  }


}
