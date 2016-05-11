package sentiment

import java.io.{File, PrintWriter}
import java.util

import org.apache.spark.SparkContext
import com.kunyandata.nlpsuit.util.{TextPreprocessing, AnsjAnalyzer}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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
      "E:\\dict\\senti_dict\\user_dict.txt",
      "E:\\dict\\senti_dict\\neg_dic.txt",
      "E:\\dict\\senti_dict\\nega_dic.txt",
      "E:\\dict\\senti_dict\\posi_dic.txt",
      "E:\\dict\\senti_dict\\user_dict.txt"
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
    TextpreWithAnsj.init(sc)
    var count = 0
    //读取文件
    val files = Util.readfile2HashMap(posPath)
    val it = files.keySet.iterator
    val negrdd = sc.textFile(negfile)
    val all=negrdd.count()+files.size()

    val segNegArr = negrdd.map(element =>{
      val temp=TextPreprocessing.removeStopWords(AnsjAnalyzer.cut(element), stopWords)
      val tempcontent=array2Str(temp)
      println("还剩下："+ (all - count))
      count +=1
      ("1#"+tempcontent+"\n")
    }).collect()

    val writer = new PrintWriter(new File(outPath), "UTF-8")
    var posArr=ArrayBuffer[String]()

    while (it.hasNext) {
      val file = it.next()
      val title = file.getName.substring(0, file.getName.indexOf(".txt"))
      val content = files.get(file).toString

      println("还剩下：" + (all - count) + "  [" + file.getParentFile.getName + "]  " + title)
//      var seg = AnsjAnalyzer.cut(title+","+content)
      var seg = AnsjAnalyzer.cut(content)
      seg = TextPreprocessing.removeStopWords(seg, stopWords)
      var str = array2Str(seg)
      posArr +="4#"+str+"\n"
      count += 1
    }
    posArr ++=segNegArr
    posArr.toArray.foreach(line =>writer.append(line.toString))
    writer.close()
  }



  def textPre_content_FS(sc:SparkContext,dataPath:String,outPath_F:String,outPath_S:String,stopWordsPath:String): Unit ={
    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer1=new PrintWriter(new File(outPath_F),"UTF-8")
    val writer2=new PrintWriter(new File(outPath_S),"UTF-8")

    //读取文件
    val files=Util.readfile2HashMap(dataPath)
    val it=files.keySet.iterator

    //初始化
    TextpreWithAnsj.init(sc)

    //计数
    var count=0
    var count_neg=0;
    var count_neu=0;
    var count_pos=0;

    val neu_num=randomNum(1529,893)
    val pos_num=randomNum(1757,893)

    while(it.hasNext){

      //取出单篇文章
      val file=it.next()

      //获取单篇文章title和content
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=files.get(file).toString

      //计数，便于查问题，title便于知道是哪篇文章
      println("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      var content_seg=AnsjAnalyzer.cut(content)
      content_seg=TextPreprocessing.removeStopWords(content_seg,stopWords)
      var contentstr=array2Str(content_seg)

      val label=file.getParentFile.getName
      label match {
        case "neg" =>{
          writer1.append(1+"#"+contentstr+"\n")
        }
        case "neu" =>{
          if(neu_num.contains(count_neu)){
            writer1.append(4+"#"+contentstr+"\n")
          }
          writer2.append(2+"#"+contentstr+"\n")
          count_neu +=1
        }
        case "pos" =>{
          if(pos_num.contains(count_pos)){
            writer1.append(4+"#"+contentstr+"\n")
          }
          writer2.append(3+"#"+contentstr+"\n")
          count_pos +=1
        }
      }
      writer1.flush()
      writer2.flush()

      //计数
      count +=1
    }
    writer1.close()
    writer2.close()
  }

  /**
    * 随机函数，获取指定范围的n个随机数
    *
    * @param len  范围  0-len
    * @param n  随机数个数
    * @return  随机数list
    * @author zhangxin
    */
  def randomNum(len:Int,n:Int): util.ArrayList[Integer] ={
    val r = new Random()
    var list = new util.ArrayList[Integer]()
    var i=0
    while(list.size() <= n){
      i = r.nextInt(len)
      if(!list.contains(i)){
        list.add(i)
      }
    }
    list
  }
}
