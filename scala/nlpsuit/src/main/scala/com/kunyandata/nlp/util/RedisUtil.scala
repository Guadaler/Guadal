package com.kunyandata.nlp.util

import org.apache.spark.SparkContext
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable.Map
import scala.util.parsing.json.JSONObject

/**
  * Created by Liu on 2016/4/13.
  */

object RedisUtil {

  /**
    * 连接 redis
    *
    * @param sc  SparkContext对象
    * @param file  redis信息文件
    * @return  返回jedis资源
    * @author  liumiao
    */
  def getRedis(sc:SparkContext, file:String): Jedis ={
    // get redis info
    val info = sc.textFile(file).collect()
    // set the parameters
    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)
    // set the redis Host port password and database
    val redisHost = info(0)
    val redisPort = info(1).toInt
    val redisTimeout = 30000
    val redisPassword = info(2)
    val redisDatabase = info(3).toInt
//   connect
    val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, redisPassword, redisDatabase)
    val jedis = pool.getResource()
//   close JedisPool
    pool.close()
    jedis
  }


  /**
    * 写redis
    *
    * @param jedis  jedis资源
    * @param name  存储的表名
    * @param result  待存储序列
    * @author  liumiao
    */
  def writeToRedis(jedis: Jedis, name:String, result:Map[String, String]): Unit ={
    for(i <- result.keys){
      jedis.hset(name, i, result(i))
      //设置表的生存时间（60秒*60分*48小时）
//      jedis.expire(name, 60 * 60 * 48)
    }
  }


//  /**
//    * 将存储信息转换为json字符串
//    *
//    * @param classify  类别信息
//    * @param p  正向新闻条数
//    * @param n  负向新闻条数
//    * @param m  中性新闻条数
//    * @param sum  当前类别新闻总条数
//    * @return  json字符串
//    * @author  liumiao
//    */
//  def toJSON(classify:String, p:Int, n:Int, m:Int, sum:Float):String = {
//    var infoMap:Predef.Map[String, Float] = Predef.Map()
//    infoMap += ("positive_percent" -> p/sum)
//    infoMap += ("negative_percent" -> n/sum)
//    infoMap += ("neutral_percent" -> m/sum)
//    val jsoninfo = JSONObject(infoMap).toString()
//    println(classify + " " + infoMap("positive_percent") + " " + infoMap("negative_percent") + " " + infoMap("neutral_percent"))
//    jsoninfo
//  }


  /**
    * 将存储信息转换为json字符串
    *
    * @param classify  类别信息
    * @param n  负向新闻条数
    * @param p_m  正向和中性新闻条数
    * @param sum  当前类别新闻总条数
    * @return  json字符串
    * @author  liumiao
    */
  def toJSON(classify:String,n:Int, p_m:Int, sum:Float):String = {
    var infoMap:Predef.Map[String, Float] = Predef.Map()
    infoMap += ("negative_percent" -> n/sum)
    infoMap += ("neu_pos_percent" -> p_m/sum)
    val jsoninfo = JSONObject(infoMap).toString()
    println(classify +" " + infoMap("negative_percent") + " " + infoMap("neu_pos_percent"))
    jsoninfo
  }

}
