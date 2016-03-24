package com.kunyandata.nlpsuit.sentiment

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.language.postfixOps;

/**
  * Created by zx on 2016/3/8
  * redis连接测试
  */

object Redis extends App{

  override def main(args: Array[String]) {
//    var conf=new SparkConf().setAppName("News_title_sentiment_lm").setMaster("local")
//    val sc = new SparkContext(conf)

      var jedis=get_redis()

      if(jedis ==null){
          println("jedis为空")
      }else{
          println("jedis不为空")
      }

    //向jedis添加数据
//    jedis.set("Name","Guadal")
//    println(jedis.get("Name"))
//
//    jedis.del("Name")
//
//    println("")


    //遍历redis
//    Set s=jedis.keys("*")
//    util.Set s2=jedis.keys("*")

//    println(jedis.hget("senti_Industry_20160226"))
//    println(jedis.hget("senti_Section_20160226"))
//    println(jedis.hget("senti_Stock_20160226"))
//    println(jedis.hget("Section_20160309"))

//    println(jedis.hget("Section_20160309","600870"))
    println(jedis.hget("senti_Industry_20160226","房地产开发"))
    println(jedis.hget("senti_Section_20160226","节能环保"))

    var result=jedis.hget("senti_Section_20160226","节能环保")
    println("节能环保："+result)
  }

  /**
    * 连接redis
    */
  def get_redis():Jedis ={
      // set the parameters
      val config: JedisPoolConfig = new JedisPoolConfig
      config.setMaxWaitMillis(10000)
      config.setMaxIdle(10)
      config.setMaxTotal(1024)
      config.setTestOnBorrow(true)

      // set the redis Host port password
      val redisHost = "222.73.34.96"
      val redisPort = 6379
      val redisTimeout = 100000
      val redisPassword ="7ifW4i@M"

      // connect
      //    val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, redisPassword, redisDatabase)
      val pool2=new JedisPool(config,redisHost, redisPort, redisTimeout, redisPassword,8)
      val jedis = pool2.getResource()

      //返回jedis对象
      jedis
  }
}
