package com.kunyandata.nlpsuit.sentiment

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2016/3/18.
  *
  * 1、用刘妙基于情感词典的方法对MySQL中前3000样本进行初步情感分析
  *
  * 2、对初步结果进一步人工调整，得到可用标注集
  */
object Action_tagging_I {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("News_title_sentiment_lm").setMaster("local")
    //SparkContext 是spark的程序入口，相当于熟悉的‘main’函数。它负责链接spark集群、创建RDD、创建累加计数器、创建广播变量。
    val sc = new SparkContext(conf)

    //加载用户词典
    title_senti.add_userdic(sc, "E:\\dict\\senti_dict\\user_dict.txt")

    // read dicts  读取词典
    val posi_dic =title_senti.read_dic(sc, "E:\\dict\\senti_dict\\posi_dic.txt")
    val nega_dic =title_senti.read_dic(sc, "E:\\dict\\senti_dict\\nega_dic.txt")
    val f_dic =title_senti.read_dic(sc, "E:\\dict\\senti_dict\\neg_dic.txt")

    //连接MySQL，并取出前3000条新闻
    var conn=MySQL.getConn();
    var allnews=MySQL.getNews(conn);

    //情感词典分析
    var count_p=0  //积极总数
    var count_n=0
    var count_m=0
    var count_notsure=0
    var count_l=0;  //漏掉

    var outpath_p="D:/Test/tagging/pos"
    var outpath_n="D:/Test/tagging/neg"
    var outpath_m="D:/Test/tagging/neu"
    var outpath_notsure="D:/Test/tagging/notsure"

    for(news <-allnews.keys){
      var p=0;
      var n=0;
      var m=0;
      var sum=0.0f;

      var title=news
      var content=allnews.get(news).get.trim();

      //计算标题情感值
      var title_cut=title_senti.cut(title);
      val title_value =title_senti.search_senti(title_cut, posi_dic, nega_dic, f_dic)
      if (title_value > 0) {
        p = p + 1
      }
      else if (title_value < 0) {
        n = n + 1
      }
      else {
        m = m + 1
      }

//      print(title+"："+title_value+" ")

      //计算文章内容的情感值
      var ss=content.split("，|。")
      for(j <-Range(0,ss.length)){
        var sentence=ss(j)
        var sentence_cut=title_senti.cut(sentence)
        val sentence_value =title_senti.search_senti(sentence_cut, posi_dic, nega_dic, f_dic)
        if (sentence_value > 0) {
          p = p + 1
        }
        else if (sentence_value < 0) {
          n = n + 1
        }
        else {
          m = m + 1
        }
      }

      sum=p-n+m*0;
//      println(" "+sum+"= "+p+" -"+n+" +"+m);

//      title.split("\\");
      var title2=Util.replace(title);

      if(p<5 && n<5 || sum==0){
          //为中性
        count_m +=1;
        var outpath=outpath_m+"/"+title2+".txt";
        IO.writefile(outpath,content);
      }else  if(p>=5 && sum>0){
        //为position
        count_p +=1
        var outpath=outpath_p+"/"+title2+".txt"
        IO.writefile(outpath,content);
      }else if(n>=5 && sum<0){
        //为消极
        count_n +=1;
        var outpath=outpath_n+"/"+title2+".txt";
        IO.writefile(outpath,content);
      }else{
        count_notsure +=1;
        var outpath=outpath_notsure+"/"+"【"+sum+"="+p+"-"+n+"】"+title2+".txt";
        IO.writefile(outpath,content);
      }

    }

    println("积极有："+count_p)
    println("消极有："+count_n)
    println("中性有："+count_m)
    println("不确定有："+count_notsure)
    println("漏掉："+count_l)


  }
}
