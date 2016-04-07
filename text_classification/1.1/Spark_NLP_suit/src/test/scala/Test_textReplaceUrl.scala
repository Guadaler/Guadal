import java.io._
import java.sql.Connection

import com.kunyandata.nlpsuit.sentiment.{TextPre_KunAnalyzer, Util}
import com.kunyandata.nlpsuit.util.WordSeg
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

import scala.io.Source

/**
  * Created by zx on 2016/3/29.
  *
  * 1.title ->url    Map[title,label]  -> Map[url,label]
  * 2.正文分词寻找     Map[url,content_seg]
  *
  * 3.对2227条未能正确查找的数据集，进行重新分词处理
  */
class Test_textReplaceUrl extends  FlatSpec with Matchers {
  "test " should "work" in {

    //将数据集做成几个label_title.txt和label_url_title.txt文件
    val filepath ="E:\\data\\data"
//    val titleLabel_map=getFile(filepath)
////    println("titleLabel_map "+titleLabel_map.size)
//
//    val con=Util.getConn()
//    var count=0
//    var a=0
//    var b=0
//
//    var urlLabel_Map=Map[String,String]()
//    var error=Map[String,String]()
//
//    val label_url_title="D:\\000_DATA\\out_2\\label_url_title.txt"
//    var writer=new PrintWriter(new File(label_url_title),"UTF-8")
//
//    //测试是否有误
//    var urlmap=Map[String,String]()
//    var titlemap=Map[String,String]()
//    var url_error=Map[String,String]()
//
//
//    for(title <-titleLabel_map.keys){
//      val label=titleLabel_map(title).toString
//      val url=getUrl(title,con)
//      if(url!=""){
//        a +=1
//        urlLabel_Map +=(url->label)
//        writer.write(label+"#"+url+"&"+title+"\n")
//        writer.flush()
//
//        if(urlmap.contains(url)){
//          url_error +=(url->title)
//        }else{
//          urlmap +=(url->title)
//        }
//
//      }else{
//
//        //对于找不到的title
//        val title2 =title.replace("每","/")
//        val url2=getUrlLike(title2,con)
//        if(url2 !=""){
//          a +=1
//          urlLabel_Map +=(url2->label)
//          writer.write(label+"#"+url2+"&"+title+"\n")
//          writer.flush()
//
//          urlmap +=(url2->title)
//        }else{
//          b +=1
//          error +=(title->label)
//        }
//      }
//      println("还剩下"+(titleLabel_map.size-count)+"  【正确】"+a+"   【错误】"+b)
//      count +=1
//    }
//    for(title <-error.keys){
//      writer.write(error(title)+"#"+title+"\n")
//      writer.flush()
//    }
//    writer.close()
//    con.close()
//    println("总共有 "+titleLabel_map.size)
//    println("【成功匹配】 "+urlLabel_Map.size+"    【匹配失败】"+error.size)
//
//    //测试错误
//    println("url正常 "+urlmap.size+"  url错误 "+url_error.size)
//    for(url <-url_error.keys){
//      println(url+"   "+url_error(url))
//    }

    //解决部分标题找不到url
//    val path2="D:\\000_DATA\\out_url\\label_title_NoUrl.txt"
//    val outpath="D:\\000_DATA\\out_url\\label_title_NoUrl_out.txt"
//    val file2=new File(path2)
//    println(file2.getAbsolutePath)
//    var writer2=new PrintWriter(new File(outpath),"UTF-8")
//
//    //解决乱码
//    val reader =new  BufferedReader(new InputStreamReader(new FileInputStream(file2),"gbk"));
//    var line=""
//    while((line=reader.readLine())!= null){
//      val label=line.substring(0,line.indexOf("#"))
//      var title=line.substring(line.indexOf("#")+1,line.length)
//      title =title.replace("每","/")
//      val url=getUrl(title,con)
//
//      println(label+"  "+title+"   "+url)
//      writer2.write(label+"#"+url+"&"+title+"\n")
//      writer2.flush()
//    }
//    writer2.close()


    //正文分词寻找     Map[url,content_seg]
//    val sourcedata="D:\\000_DATA\\seg_QQ\\segTrainingSet"
//    val label_url_titl="D:\\000_DATA\\out\\label_url_title.txt"
    val outpath1="D:\\000_DATA\\out\\【6069】url_title.txt"
//    val outpath2="D:\\000_DATA\\out\\url_label.txt"
//    val outpath3="D:\\000_DATA\\out\\url_content.txt"
//    val outpath4="D:\\000_DATA\\out\\label_content.txt"
//
//    val writer1=new PrintWriter(new File(outpath1),"UTF-8")
//    val writer2=new PrintWriter(new File(outpath2),"UTF-8")
//    val writer3=new PrintWriter(new File(outpath3),"UTF-8")
//    val writer4=new PrintWriter(new File(outpath4),"UTF-8")
//
//    //取QQ数据，封装到map
//    var url_content_map=Map[String,String]()
//    val file=new File(sourcedata)
//    for(line <-Source.fromFile(file).getLines()){
//      val url=line.substring(0,line.indexOf(".shtml")+6)
//      val content=line.substring(line.indexOf(".shtml")+6,line.length)
//      url_content_map +=(url->content)
//    }
//
//    //取本地label_url_title数据
//    var label_url_map=Map[String,String]()
//    val file3=new File(label_url_titl)
//    for(line <-Source.fromFile(file3).getLines()){
//      val label=line.substring(0,line.indexOf("#"))
//      val url=line.substring(line.indexOf("#")+1,line.indexOf("&"))
//      val title=line.substring(line.indexOf("&")+1,line.length)
//
//      writer1.write(url+"#"+title+"\n")
//      writer2.write(url+"#"+label+"\n")
//      writer1.flush()
//      writer2.flush()
//
//      if(label_url_map.contains(url)){
//        println("[URL] "+url)
//      }else{
//        label_url_map +=(url->label)
//      }
//    }
//
//    writer1.close()
//    writer2.close()
//
//    //在QQ中查找数据
//    //计数
//    var a=0
//    var b=0
//
//    for(url <- label_url_map.keys){
//      if(url_content_map.keySet.contains(url)){
//        a +=1
//        val label=label_url_map(url)
//        val content=url_content_map(url)
//
//        writer3.write(url+"#"+content+"\n")
//        writer4.write(label+"#"+content+"\n")
//        writer3.flush()
//        writer4.flush()
//      }else{
//        println(url)
//        b +=1
//      }
//    }
//    writer3.close()
//    writer4.close()
//    println("[总共有] "+label_url_map.size+"  [匹配成功] "+a+"  [失败] "+b)


    //读取unmatch
   /* val url_unmatch="D:\\000_DATA\\out\\url_unmacth_2227.txt"
    var url_map=Map[String,String]()
    for(line <-Source.fromFile(new File(url_unmatch)).getLines()){
      url_map +=(line ->" ")
    }

    //读取unmatch的标题
    var title_url_map=Map[String,String]()
    for(line <-Source.fromFile(new File(outpath1)).getLines()){
      val url3=line.substring(0,line.indexOf("#"))
      val title3=line.substring(line.indexOf("#")+1,line.length)
      if(url_map.keySet.contains(url3)){
        title_url_map +=(title3->url3)
      }
    }

    println("url_map "+url_map.size+" url_title_map: "+title_url_map.size)

    //读取数据集，找到对应的，，分词，，写入

    val outpath5="D:\\000_DATA\\out\\【2227】url_content.txt"
    val outpath6="D:\\000_DATA\\out\\【2227】label_content.txt"
    val outpath7="D:\\000_DATA\\out\\【2227】error.txt"
    val writer5=new PrintWriter(new File(outpath5),"UTF-8")
    val writer6=new PrintWriter(new File(outpath6),"UTF-8")
    val writer7=new PrintWriter(new File(outpath7),"UTF-8")

    //计数
    var count=0
    println("总共有 "+title_url_map.size)

    val files=Util.readfile2HashMap(filepath)
    var titleLabel_Map=Map[String,String]()
    val it=files.keySet().iterator()

    //分词器
    val conf = new SparkConf().setAppName("mltest").setMaster("local")
    val sc = new SparkContext(conf)
    val stopWordsPath="E:\\data\\stop_words_CN"

    while(it.hasNext){
      val file=it.next()
      val label=Util.getLabel(file)
      val title4=file.getName.substring(0,file.getName.indexOf(".txt"))
      var content=files.get(file).trim

      if(title_url_map.contains(title4)){
        count +=1
        print("还剩下 "+(title_url_map.size-count)+"  已处理 "+count+"   ")
        println("【正在处理】 "+label+"  "+title4)

        val contentstr =TextPre_KunAnalyzer.textPre(sc,content,stopWordsPath)
        if(contentstr ==null){
          writer7.write(title_url_map(title4)+"#"+title4+"\n")
          writer7.flush()
        }else{
          writer5.write(title_url_map(title4)+"#"+contentstr+"\n")
          writer6.write(label+"#"+contentstr+"\n")
          writer5.flush()
          writer6.flush()
        }

      }

    }*/






    //统计【3842】label_content.txt数据是否平衡
    val filePath="D:\\000_DATA\\out\\【3842】label_content.txt"
    //将label 转成 labelNum
    val fileOutPath="D:\\000_DATA\\out\\【3842】labelNum_content.txt"
//    countLabel(filePath,fileOutPath)

    //去掉中性数据  并转成 labelNum  neg_pos
    val fileOutPath2="D:\\000_DATA\\out\\【3842】labelNum_content_noNeu.txt"

    //去掉消极，留下积极和中性
    val fileOutPath3="D:\\000_DATA\\out\\【3842】【Neu+pos】labelNum_content.txt"
    removedNeu(filePath,fileOutPath3)



  }

  //----------------------------------------------------------------

  def getFile(filePath:String): Map[String,String] ={
    val files=Util.readfile2HashMap(filePath)
    var titleLabel_Map=Map[String,String]()
    val it=files.keySet().iterator()
    while(it.hasNext){
      val file=it.next()
      val label=Util.getLabel(file)
      val title=file.getName.substring(0,file.getName.indexOf(".txt"))

      if(titleLabel_Map.contains(title)){
        println(label+"  【已标注】"+titleLabel_Map(title)+"  "+title)
        titleLabel_Map.-(title)
      }else{
        titleLabel_Map +=(title -> label)
      }
    }
    titleLabel_Map
  }

  def getUrl(str:String,con:Connection): String={
    val sqlstr="select url FROM indus_text_with_label WHERE title ='"+str+"'"
    var url=""
    val statement = con.createStatement()
    val resultSet = statement.executeQuery(sqlstr)
    while ( resultSet.next() ) {
      url= resultSet.getString("url").trim()
    }
    url
  }

  def getUrlLike(str:String,con:Connection): String={
    val sqlstr="select url FROM indus_text_with_label WHERE title like '%"+str+"%'"
    var url=""
    val statement = con.createStatement()
    val resultSet = statement.executeQuery(sqlstr)
    while ( resultSet.next() ) {
      url= resultSet.getString("url").trim()
    }
    url
  }

  //统计【3842】label_content.txt数据是否平衡，并进行 【标签<->编号】 替换，写出到本地文件
  def countLabel(filePath:String,fileOutPath:String): Unit ={
    //计数
    var count=0
    var neg_count=0
    var neu_count=0
    var pos_count=0
    //写入
    val writer=new PrintWriter(new File(fileOutPath),"UTF-8")

    for(line <-Source.fromFile(new File(filePath)).getLines()){
      count +=1
      val label=line.substring(0,line.indexOf("#"))
      val content=line.substring(line.indexOf("#")+1,line.length)

      label match {
        case "neg"   => {
          neg_count +=1
          writer.write("1#"+content+"\n")
          writer.flush()
        }
        case "neu"   => {
          neu_count +=1
          writer.write("2#"+content+"\n")
          writer.flush()
        }
        case "pos"    => {
          pos_count +=1
          writer.write("3#"+content+"\n")
          writer.flush()
        }
      }

    }

    println("总共 "+count)
    println("消极： "+neg_count+"    中性: "+neu_count+"   积极 ："+pos_count)
  }

  //去掉中性数据  并转成 labelNum，即进行 【标签<->编号】 替换，写出到本地文件
  def removedNeu(filePath:String,fileOutPath:String): Unit ={
    //计数
    var count=0
    var neg_count=0
    var neu_count=0
    var pos_count=0
    //写入
    val writer=new PrintWriter(new File(fileOutPath),"UTF-8")

    for(line <-Source.fromFile(new File(filePath)).getLines()){
      count +=1
      val label=line.substring(0,line.indexOf("#"))
      val content=line.substring(line.indexOf("#")+1,line.length)

      label match {
        case "neg"   => {
//          neg_count +=1
//          writer.write("1#"+content+"\n")
//          writer.flush()
        }
        case "neu"   => {
          neu_count +=1
          writer.write("2#"+content+"\n")
          writer.flush()
        }
        case "pos"    => {
          pos_count +=1
          writer.write("3#"+content+"\n")
          writer.flush()
        }
      }

    }

    println("总共 "+count)
    println("消极： "+neg_count+"    中性: "+neu_count+"   积极 ："+pos_count)
  }
}
