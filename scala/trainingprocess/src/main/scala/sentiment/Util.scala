package sentiment

import java.io.{ObjectInputStream, File, PrintWriter}
import java.sql.{Connection, DriverManager}
import java.util

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.ansj.domain.Term
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable.HashMap
import scala.io.Source

/**
  * Created by zhangxin on 2016/3/17.
  * 此为zhangxin自用工具类，或作方法留存
  */
object Util {

  /**
    * 根据完整文件名提取文件类别
    *
    * @param file  文件名  E：\data\pos\****.txt
    * @return  类别
    */
  def getLabel(file:File): String ={
    val parentPath = file.getParentFile()
    val label = parentPath.getName()
    label
  }

  /**
    * 标题中非法字符替换
    *
    * @param illegalTitle  替换前标题
    * @return  替换后标题
    */
  def replace(illegalTitle:String):String={

    val title = illegalTitle.replace("/","每")
      .replace("|", "：")
      .replace(":", "：")
      .replace("\"", "“")
      .replace("?", "？")
      .replace("*", "")

    title
  }

  /**
    * 替换文章非法字符，否则分词器不能分词，导致程序中断
    * 非法字符包括  \ / * ? : "<> |
    *
    * @param illegalStr  替换前文本
    * @return  替换后文本
    * @author zhangxin
    */
  def replaceIllegal(illegalStr: String): String = {

    val str = illegalStr.replace("\\","每")
      .replace("/", "每")
      .replace("|", "：")
      .replace("：", "：")
      .replace("\"", "“")
      .replace("?", "？")
      .replace("<", "《")
      .replace(">", "》")
      .replace("*", "》")
      .replace("\n", " ")
      .replace("\t", " ")
      .replace(",", "，")

    str
  }

  //----------【IO操作】-------------------------------------------------------
  /**
    * 读取文件，【用scala Map存储】
    *
    * @param path  文件的父目录的路径，注意是单层循环
    * @return  所有文章map[File,content]
    * @author zhangxin
    */
  def readFile2Map(path: String): Map[File, String] = {

    var file_map = Map[File, String]()
    val files = new File(path).listFiles()   //获取父目录文件列表
    for(file <- files){
      val content = Source.fromFile(file).getLines().map(line => {line}).mkString
      file_map += (file ->content)
    }

    println("总共读取："+file_map.size+" 条")
    file_map
  }

  /**
    *读取文件， 【用Java HashMap存储】
    *
    * @param path  文件夹的父目录路径，注意下面用了两层循环
    * @return  新闻Map[File,content]
    * @author zhangxin
    */
  def readFile2HashMap(path: String): util.HashMap[File, String]={
    val file_map =new util.HashMap[File,String]()
    //获取父目录文件列表
    val catDir=new File(path).listFiles()
    for(dir <-catDir){
      val files=dir.listFiles()
      println(dir+"   共 "+files.length+" 篇")
      for(file <- files){
        val content = Source.fromFile(file).getLines().map(line =>{line}).mkString
        file_map.put(file, content)
      }
    }

    println("Read Over!! 总共读取："+file_map.size+" 条")
    file_map
  }

  /**
    * 写入文件
    *
    * @param outPath  写入文件路径
    * @param content  写入内容
    * @author zhangxin
    */
  def writeFile(outPath: String, content: String): Unit ={
    val writer = new PrintWriter(new File(outPath), "UTF-8")
    writer.write(content)
    writer.flush()
    writer.close()
  }

  //----------【MySQL】--------------------------------------
  /**
    * 获取MySQL连接
    *
    * @return  connection
    * @author zhangxin
    */
  def getConn(): Connection ={
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.1.14:3306/stock"
    val username = "root"
    val password = "root"
    var connection:Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    }catch {
      case e => e.printStackTrace
    }
    connection
  }

  //-----------【redis】---------------------------------
  /**
    * 连接redis
    *
    * @author zhangxin
    */
  def get_redis():Jedis ={
    // 参数设置
    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)

    // 设置redis的路径、端口、密码
    val redisHost = "222.73.34.96"
    val redisPort = 6379
    val redisTimeout = 100000
    val redisPassword ="7ifW4i@M"

    // 连接，“8”表示连接redis中 8 号库
    val pool=new JedisPool(config, redisHost, redisPort, redisTimeout, redisPassword, 8)
    val jedis = pool.getResource()

    //返回jedis对象
    jedis
  }

  //---------【计算TF—IDF】-------------------------------
  /**
    * 计算TF-Idf
    *
    * @param item  关键词
    * @param article  关键词所在文章
    * @param filesMap 所有“文章——关键词”Map
    * @return Tf-idf
    * @author zhangxin
    */
  def getTf_Idf(item:String, article:HashMap[String, Int], filesMap:Array[HashMap[String, Int]]): Double ={
    val tf=getTf(item, article)
    val idf=getIdf(item:String,filesMap)
    val Tf_Idf=tf*idf

    Tf_Idf
  }
  /**
    * 计算tf
    *
    * @param item  关键词
    * @param article  关键词所在文章
    * @return tf值
    * @author  zhangxin
    */
  def getTf(item: String, article: HashMap[String, Int]): Double = {

    //该词词频
    val count = article(item)
    //该文章所有词数
    val sum = article.values.reduce((x,y) => x+y)

    val tf:Double = if(sum != 0){
      count.toDouble/sum.toDouble
    }else{
      0.0000
    }

    tf
  }

  /**
    * 计算IDF
    *
    * @param item  关键词
    * @param filesMap 所有“文章——关键词”Map
    * @return IDF
    * @author zhangxin
    */
  def getIdf(item: String, filesMap: Array[HashMap[String, Int]]): Double = {

    var count = 0

    filesMap.foreach(file => {
      if(file.keySet.contains(item)) count += 1
    })
    val idf = Math.log(filesMap.size.toDouble/count.toDouble)

    idf
  }

  /**
    * 对每篇文章进行词频统计
    *
    * @param file  文章内容分词结果数组（已分词、去停、数组）
    * @return 单词-词频
    */
  def countWord(file: Array[String]): HashMap[String, Int] = {
    var wordmap = new HashMap[String,Int]
    file.foreach(word => {
      if(wordmap.keySet.contains(word)) {
        wordmap(word) += 1
      }else{
        wordmap += (word -> 1)
      }
    })
    wordmap
  }

  //---------------【文本处理】----------------------------------
  /**
    * 对分词数据集中存在部分数据缺失进行剔除，  如剔除1#null
    *
    * @param file  输入文件
    * @param out  重新写出文件
    */
  def textPro(file: String, out: String): Unit = {
    val wr=new PrintWriter(out,"UTF-8")
    var count=0
    for(line <-Source.fromFile(new File(file)).getLines()){
      val temp = line.split("#")
      if(temp.length ==1){  //如果不这样判断，会报下标溢出
        count +=1
      }else{
        var temp2=""
        if(temp(1).startsWith(",")){
          temp2 =temp(1).substring(1,temp(1).length)
        }else if(temp(1).startsWith(", ")){
          temp2 =temp(1).substring(2,temp(1).length)
        }else if(temp(1).startsWith(" ,")){
          temp2 =temp(1).substring(2,temp(1).length)
        }else{
          temp2 =temp(1)
        }
        if(temp2.contains(",,")){
          temp2=temp2.replace(",,",",")
        }
        println(temp(0)+"#"+temp2+"\n")
        wr.write(temp(0)+"#"+temp2+"\n")
        wr.flush()
      }

    }
    println("【无数据有】"+count)
    wr.close()
  }

  /**
    * 根据[词性]剔除无用词
    *
    * @param words 分词结果数组
    * @return 处理结果Map[String,Int]  [关键词，词频]
    * @author zhangxin
    */
  def removeUsenelss(words: Array[Term]): util.HashMap[String, Int] = {
    val words_fre_map = new util.HashMap[String,Int]()
    if(words != null){
      for(word: Term <- words){
        val wordstr = word.toString()
        if(!wordstr.contains("##") && wordstr.contains("/")){
          val item = wordstr.substring(0,wordstr.indexOf("/"))
          val ext = wordstr.substring(wordstr.indexOf("/")+1,wordstr.length())
          if(!ext.startsWith("uj")&& !ext.startsWith("ul")&& !ext.startsWith("w") && !ext.startsWith("m")){
            //将符合条件的item添加到words_fre_map中
            if(!words_fre_map.containsKey(item)){
              words_fre_map.put(item, 1)
            }else{
              words_fre_map.put(item, words_fre_map.get(item)+1)
            }

          }
        }
      }
    }
    words_fre_map
  }

  /**
    * 将词加入到“词典”[wordsDict]
    *
    * @param wordsDict  词典格式：  编号:关键词
    * @param words_fre_map  一篇文章  关键词：tf-idf值
    */
  def add2wordsDict(wordsDict: util.HashMap[String, Int], words_fre_map: util.HashMap[String,Int]): Unit = {
    var count = wordsDict.size()
    val it = words_fre_map.keySet().iterator()
    while(it.hasNext){
      count += 1
      val word = it.next()
      if(!wordsDict.keySet().contains(word)){
        wordsDict.put(word, count)
      }
    }
  }

  /**
    * 对长文章进行分段“分词+去停+格式化”，再连接成一个字符串返回
    *
    * @param content  待处理文章
    * @param stopWords 停用词表
    * @return 拼接后结果字符串
    * @author zhangxin
    */
  def bigText(content:String,stopWords:Array[String],kunyanConfig:KunyanConf): String ={

    var result=""
    //处理整数部分，即 0 ~ 1500*（n-1）部分
    val n=content.length/1500+1
    for(i <- Range(1,n)){
      //截取
      val content_1=content.substring(1500*(i-1),1500*i+1)
      //分词
      val tempSeg=TextPreprocessing.process(content_1,stopWords,kunyanConfig)
      if(tempSeg !=null){
        result += ","+tempSeg.mkString(",")
      }else{
        null
      }
    }

    //处理剩下的部分
    val content_2=content.substring(1500*(n-1),content.length)
    val tempSeg=TextPreprocessing.process(content_2,stopWords,kunyanConfig)
    //将前后两部分结果进行拼接
    result += ","+tempSeg.mkString(",")
    result
  }

  /**
    * 将一个单篇文章占一行的长txt文件分别写出单个文件，逆格式化
    * 长文件格式：一行为一篇文章
    *
    * @param sc
    * @param inPath
    * @param outPath
    */
  def write2local(sc: SparkContext, inPath: String, outPath: String): Unit = {
    var count=0
    val data=sc.textFile(inPath)
    data.foreach(line => {
      val title = line.substring(0, line.indexOf("\t"))
      val content = line.substring(line.indexOf("\t")+1, line.length)
      count += 1
      val writer2 = new PrintWriter(outPath+"\\"+Util.replace(title)+".txt","utf-8")
      println("路径为： "+outPath+"\\"+title+".txt")
      writer2.write(TextPreprocessing.formatText(content))
      writer2.close()
    } )
  }

  //----------------【JSON】----------------
  /**
    * 解析JSON文件的方法类
    *
    * @param path  Json文件路径
    */
  class ParseJson(path: String) {
    //读取json文件
    private val jsObj = Source.fromFile(path).getLines().mkString("")
    //将json数据传入到一个JSON对象
    private val config = new JSONObject(jsObj)
    def getValue(key1: String, key2: String): String = {
      config.getJSONObject(key1).getString(key2)
    }
  }

  //-------------------【HDFS】---------------
  /**
    * 读取模型，从hdfs上读取
    *
    * @param modelfileFromHdfs hdfs路径 如hdfs://222.73.57.12:9000/user/F_2_1500
    * @return 模型Map[模型名称，模型]
    */
  def init(modelfileFromHdfs: String): Map[String, Any] = {
    var modelMap:Map[String, Any] = Map()
    //读取hdfs上保存的模型
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)
    val fileList = fs.listStatus(new Path(modelfileFromHdfs)).map(_.getPath.toString)
    fileList.foreach(file => {
      val modelName = file.replaceAll(modelfileFromHdfs, "")
      val tempModelInput = new ObjectInputStream(fs.open(new Path(file))).readObject()
      modelMap +=(modelName -> tempModelInput)
    })
    modelMap
  }

  //-------------------【Spark中的TF_idf】---------------
  def Spark_TFIDF(): Unit ={
    val conf=new SparkConf().setAppName("LDA").setMaster("local")
    val sc=new SparkContext(conf)

    val a=Array(
      "hello spark hello",
      "spark spark hello",
      "goodbye spark"
    )

    val ardd=sc.parallelize(a).map(_.split(" ").toSeq)
    ardd.foreach(println(_))


    val hashingTF2=new HashingTF()      //首先创建tf计算实例
    val hashingTF=new HashingTF()      //首先创建tf计算实例
    val tf=hashingTF.transform(ardd).cache()   //计算文档tf值

    //创建IDF实例
    val idf=new IDF().fit(tf)

    //计算tf_idf
    val tf_idf= idf.transform(tf)

    //输出
    tf.foreach(println(_))
    tf_idf.foreach(println(_))

  }

  //--------------------------【Spark PCA】----------------
  def spark_PCA(): Unit ={
    val conf=new SparkConf().setMaster("local").setAppName("PCATest")
    val sc = new SparkContext(conf)

    //
    val data=sc.textFile("D:\\222_TDT\\a.txt")    //创建RDD
      .map(_.split(" ")    //按照“ ”分割
      .map(_.toDouble))    //转成Double格式
      .map(line => Vectors.dense(line))  //转成Vector格式

    //
    val rm=new RowMatrix(data)
    val pc=rm.computePrincipalComponents(3)

    pc.toArray.foreach(println(_))
    //
    val mx=rm.multiply(pc)
    mx.rows.foreach(println(_))
  }

  //--------------文件操作--------------------------

  /**
    * 删除指定路径下的模型，删除模型的所有文件
    * @param path  模型路径
    * @author zhangxin
    */
  def delModel(path:String): Unit ={
    val root = new File(path)
    def deleteAll(root: File): Unit = {
      if (root.isDirectory) {
        val templist = root.listFiles()
        templist.foreach(line => {
          deleteAll(line)
          line.delete()
        })
      } else {
        root.delete()
      }
    }
    deleteAll(root)
  }


  def main(args: Array[String]): Unit = {

  }
}
