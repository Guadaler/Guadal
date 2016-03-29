package com.kunyandata.nlpsuit.sentiment

import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager}
import java.util

import com.kunyandata.nlpsuit.util.TextProcess
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.io.Source

/**
  * Created by zx on 2016/3/17.
  */
object Util {

  /**
    * 根据文件名提取文件类别
    *
    * @param file  文件名
    * @return  类别
    */
  def getLabel(file:File): String ={
    val parentPath=file.getParentFile()
    val label=parentPath.getName()
    label
  }

  /**
    * 加载类别标签
    *
    * @return 返回类别标签map
    */
  def loadlabel_map(): util.HashMap[String,Int] ={
    val label_map=new util.HashMap[String,Int]
    label_map.put("neg",1)
    label_map.put("neu",2)
    label_map.put("pos",3)
    label_map
  }

  /**
    * 替换文章非法字符，否则分词器不能分词，导致程序中断
    * 非法字符包括  \ / * ? : "<> |
    *
    * @param str_ill  替换前带非法字符文本
    * @return  替换后文本
    * @author zhangxin
    */
  def replaceIllegal(str_ill:String): String ={
    var str_leg=str_ill.replace("\\","每")
    str_leg=str_leg.replace("/","每")
    str_leg=str_leg.replace("|","：")
    str_leg=str_leg.replace("：","：")
    str_leg=str_leg.replace("\"","“")
    str_leg=str_leg.replace("?","？")
    str_leg=str_leg.replace("<","《")
    str_leg=str_leg.replace(">","》")
    str_leg=str_leg.replace("*","》")
    str_leg
  }

  //----------【IO操作】-------------------------------------------------------
  /**
    * 读取文件，【用scala Map存储】
    *
    * @param path  文件的父目录的路径，注意是单层循环
    * @return  所有文章map[File,content] ： File 文章对象  content 文章内容
    * @author zhangxin
    */
  def readfile2Map(path:String): Map[File,String]={
    var file_map = Map[File,String]()
    val files=new File(path).listFiles()   //获取父目录文件列表
    for(file <-files){
      println(file.getAbsoluteFile)
      var str:String=""
      for(line <-Source.fromFile(file).getLines()){
        str +=line
      }
      file_map +=(file ->str)
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
  def readfile2HashMap(path:String): util.HashMap[File,String]={
    var file_map =new util.HashMap[File,String]()
    val catDir=new File(path).listFiles()   //获取父目录文件列表
    for(dir <-catDir){
      val files=dir.listFiles()
      println(dir+"   共 "+files.length+" 篇")
      for(file <-files){
        var str=""
        for(line <-Source.fromFile(file).getLines()){
          str +=line
        }
        file_map.put(file,str)
      }
    }
    println("Read Over!! 总共读取："+file_map.size+" 条")
    file_map
  }

  /**
    * 写入文件
    *
    * @param outpath  写入文件路径
    * @param content  写入内容
    * @author zhangxin
    */
  def writefile(outpath:String,content:String): Unit ={
    var writer=new PrintWriter(new File(outpath),"UTF-8")
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
    val pool=new JedisPool(config,redisHost, redisPort, redisTimeout, redisPassword,8)
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
    * @param word_map 所有“文章——关键词”Map
    * @return Tf-idf
    * @author zhangxin
    */
  def getTf_Idf(item:String, article:util.HashMap[String, Int], word_map:util.HashMap[File, util.HashMap[String, Int]]): Double ={
    val tf=getTf(item:String, article:util.HashMap[String, Int])
    val idf=getIdf(item:String,word_map:util.HashMap[File, util.HashMap[String, Int]])
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
  def getTf(item:String, article:util.HashMap[String, Int]):Double ={
    var tf:Double=0.000000
    val count=article.get(item);  //该词的词频
    var sum=0;  //该文章所有词数
    val it=article.values().iterator()
    while (it.hasNext){
      val key=it.next()
      sum +=key
    }
    if(sum !=0){
      tf=article.get(item).toDouble/sum.toDouble
    }
    tf
  }

  /**
    * 计算IDF
    *
    * @param item  关键词
    * @param word_map 所有“文章——关键词”Map
    * @return IDF
    * @author zhangxin
    */
  def getIdf(item:String,word_map:util.HashMap[File, util.HashMap[String, Int]]): Double ={
    var idf:Double=0.000000;
    var count=0;
    var it=word_map.keySet().iterator()
    while (it.hasNext){
      var key=it.next();
      var onefile=word_map.get(key)
      if(onefile.keySet().contains(item)){
        count +=1;
      }
    }
    idf=Math.log(word_map.size().toDouble/count.toDouble)
    println("idf:"+idf+"="+count.toDouble+"  "+word_map.size().toDouble)
    idf
  }

  //---------------【文本处理】----------------------------------
  /**
    * 实现字符串的分词和去停,并分装成方法  ，与上面的process()方法相同，只是分词采用ansj
    *
    * @param content 需要处理的字符串
    * @param stopWordsBr  停用词
    * @return 返回分词去停后的结果
    * @author zhangxin
    */
  def process_ansj(content: String, stopWordsBr: Broadcast[Array[String]]): Array[String] = {
    // 格式化文本
    val formatedContent =TextProcess.formatText(content)
    // 实现分词
    val resultWords=Analyzer.cut(content)
    // 实现去停用词
    if (resultWords == null) null
    else TextProcess.removeStopWords(resultWords, stopWordsBr.value)
  }
}
