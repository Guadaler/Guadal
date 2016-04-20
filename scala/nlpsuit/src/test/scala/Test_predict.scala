//import com.kunyandata.nlpsuit.sentiment.{TextPre_KunAnalyzer, Analyzer, Util}
import com.kunyan.nlpsuit.sentiment.PredictWithNb
import com.kunyan.nlpsuit.util.{TextPreprocessing, WordSeg}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source
import scala.util.Random
import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2016/3/29.
  */
object Test_predict {
  def main(args: Array[String]) {
    val sconf=new SparkConf().setAppName("test").setMaster("local")
    val sc=new SparkContext(sconf)
//
//    val stopWords=Source.fromFile("D:\\111_DATA\\data\\stop_words_CN").getLines().toArray  //读取停用词典并转成Array
//    val stopWordsBr = sc.broadcast(stopWords)
//
//    //单模型+单文章预测
//    val model=PredictWithNb.init()
//    val content = "原标题：国家发改委：中国足球2030年前跻身世界强队\n"
//    val result =PredictWithNb.predictWithSigle(content, model, stopWordsBr)
//    println(result)


    //
//    val a = sc.parallelize(1 to 9, 3)
//    def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
//      var res = List[(T, T)]()
//      var pre = iter.next  while (iter.hasNext) {
//        val cur = iter.next;
//        res .::= (pre, cur) pre = cur;
//      }
//      res.iterator
//    }
//    a.mapPartitions(myfunc).collect

//    val one: PartialFunction[Int, String] = { case 1 => "one"; case _ => "other"}
//    val data = sc.parallelize(List(2,3,1))
//    data.collect().foreach(a=> println(a))
//    data.collect(one).collect.foreach(println(_))
//
//    val a = sc.parallelize(1 to 9, 3)
//    a.collect().foreach(println(_))
//
//    val b=a.map(_*2)
//    b.collect().foreach(println(_))
//
//    val c=sc.parallelize(Array[Int](1,2,3))
//    c.collect().foreach(c=>println("b: "+c))
//
//    val  d=c.map(a => (a,a*a))
//    d.collect().foreach(c=>println("c: "+c))
//    d.mapValues("$"+_+"$").collect().foreach(println(_))
//
//    a.mapPartitions(myfunc).collect.foreach(println(_))
//
//
//    val x = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)
//    x.collect().foreach(c=>println("X: "+c))
//    x.mapWith(a => a * 10)((a, b) => (b + 2)).collect.foreach(println(_))
//
//    val aa = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 3)
//    aa.flatMapWith(x => x, true)((x, y) => List(y, x)).collect.foreach(e=>println("e:"+e))
//
//    val aaa = sc.parallelize(List((1,2),(3,9),(3,4),(3,6)))
//    val bbb = aaa.reduceByKey((x,y) => x + y)
//    bbb.collect.foreach(println(_))
//
//    //用reduceByKey过滤掉重复元素，选择第一个出现的元素
//    aaa.reduceByKey((x,y)=> x).collect.foreach(println(_))

    //
//    val lines=sc.textFile("hdfs://...")
//    val arr2=Array[String]("Hello,china","Hnihao","Hello,zx","Hfanghui is stupid","Haaha")
//    val lines=sc.parallelize(arr2)
//    val error=lines.filter(_.startsWith("H"))
//    error.persist
//    val count=error.filter(_.contains("Hello")).count
//    println("count: "+count)
//    val arr=error.filter(_.contains("stupid"))
//      .map(_.split(' ')(2))
//      .collect()
//        .foreach(println(_))

    val a1=Array()

    val a=Array[Int](10)
    val a2=new Array[Int](10)

    val b1=Map[String,Int]()
    val b2=Map()

    val c1=Array(1,2,3,4,5,6)
    val c1_rdd=sc.parallelize(c1)
    val tt=c1_rdd.map(line =>(line,line*line))

    //那么t到底是tuples还是kv呢
//    t.collect.foreach()
    tt.map(line =>
      println("******************************************************************"+line._1+"  "+line._2)
    )

    tt.collect().foreach(element=>println("c: "+element))


    tt.mapValues("x" + _ + "x").collect.foreach(println(_))
  }

  def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next;
      res .::= (pre, cur)
      pre = cur;
    }
    res.iterator
  }
}
