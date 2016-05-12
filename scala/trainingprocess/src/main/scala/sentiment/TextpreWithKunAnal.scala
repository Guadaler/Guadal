package sentiment

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.SparkContext

/**
  * Created by zx on 2016/3/25. 用坤雁分词器对文本进行预处理
  */
object TextpreWithKunAnal {

  /**
    *
    * @param sc
    * @param dataPath
    * @param outPath
    * @param stopWordsPath
    */
  def textContentPre(sc:SparkContext,dataPath:String,outPath:String,stopWordsPath:String): Unit ={

    // 获取配置文件信息
    val configInfo = new SentimentConf()
    configInfo.initConfig("D:\\111_DATA\\data\\config.json")

    // 配置kunyan分词
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    val stopWords = sc.textFile(stopWordsPath).collect()
    val writer=new PrintWriter(new File(outPath),"UTF-8")
    //读取所有文章
    val files = Util.readFile2HashMap(dataPath)
    val it=files.keySet.iterator

    //计数
    var count=0
    while(it.hasNext){
      val begin = new Date().getTime
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      //取出单篇文章
      val file=it.next()

      //获取单篇文章title和content
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))
      val content=Util.replaceIllegal(files.get(file).toString)

      //计数，便于查问题
      print("还剩下："+(files.size()-count)+"  ["+file.getParentFile.getName+"]  "+title)

      val content_seg=TextPreprocessing.process(content,stopWords,kunyanConfig)
      val contentstr=content_seg.mkString(",")

      //处理content，注意对长文本要先分段再分词
//      var contentstr=""
//      if(content.size>1500){
//        contentstr=bigText(content,stopWords,kunyanConfig)
//      }else{
//        val content_seg=TextPreprocessing.process(content,stopWords,kunyanConfig)
//        contentstr=content_seg.mkString(",")
//      }

      //获取文章类别编号
      val label=file.getParentFile.getName
      label match {
        case "neg" => writer.append("1#"+contentstr+"\n").flush()
        case "neu" => writer.append("4#"+contentstr+"\n").flush()
        case "pos" => writer.append("4#"+contentstr+"\n").flush()
      }

      //计数
      count +=1
      val end=new Date().getTime
      println("    【耗时】 "+(end-begin))
    }
    writer.close()
  }
  
  def textPre(sc:SparkContext,content:String,stopWordsPath:String): String ={
    // 获取配置文件信息
    val configInfo = new SentimentConf()
    configInfo.initConfig("D:\\111_DATA\\data\\config.json")

    // 配置kunyan分词
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    val stopWords = sc.textFile(stopWordsPath).collect()
    //非法字符替换，所有title已处理过，因此无需替换
    val content2 =Util.replaceIllegal(content)
    //处理content，注意对长文本要先分段再分词
    var contentstr=""
    if(content2.size>1500){
      contentstr=bigText(content2,stopWords,kunyanConfig)
    }else{
      val content_seg=TextPreprocessing.process(content,stopWords,kunyanConfig)
      if(content_seg ==null){
        return null
      }else{
        for(word <-content_seg){
          contentstr +=" "+word.toString()
        }
      }
    }
    contentstr
  }

  /**
    * 对长文章进行分段“分词+去停+格式化”，再连接成一个字符串返回
    *
    * @param content  待处理文章
    * @param stopWords 停用词表
    * @return 连接后结果字符串
    * @author zhangxin
    */
  def bigText(content:String,stopWords:Array[String],kunyanConfig:KunyanConf): String ={

    var contentstr=""

    //处理整数部分，即 0 ~ 1500*（n-1）部分
    val n=content.size/1500+1
    for(i <- 1 until n){
      //截取
      val content_i=content.substring(1500*(i-1),1500*i+1)
      //分词
      val content_seg=TextPreprocessing.process(content,stopWords,kunyanConfig)
      if(content_seg ==null){
        return null
      }else{
        //结果Array =>String
        contentstr=content_seg.mkString(",")
      }
    }

    //处理剩下的部分
    val content_j=content.substring(1500*(n-1),content.size)
    val content_seg=TextPreprocessing.process(content_j,stopWords,kunyanConfig)
    contentstr +=","+content_seg.mkString(",")
    contentstr
  }

}
