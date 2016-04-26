package com.kunyan.util

import org.apache.spark.SparkContext
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

/**
  * Created by Liu on 2016/4/13.
  */

object RedisUtil {

  /**
    * 连接 redis
    *
    * @param info redis信息
    * @return  返回jedis资源
    * @author  liumiao
    */
  def getRedis(info: Array[String]): Jedis ={
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
    val jedis = pool.getResource
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
  def writeToRedis(jedis: Jedis, name:String, result:mutable.Map[String, String]): Unit ={
    for(i <- result.keys){
      jedis.hset(name, i, result(i))
    }
  }

  /**
    * 将存储信息转换为json字符串
    *
    * @param classify  类别信息
    * @param negative  负向新闻条数
    * @param positive  正向和中性新闻条数
    * @param sum  当前类别新闻总条数
    * @return  json字符串
    * @author  liumiao
    */
  def toJSON(classify:String, negative:Int, positive:Int, sum:Float):String = {
    var infoMap:Predef.Map[String, Float] = Predef.Map()
    infoMap += ("negative_percent" -> negative/sum)
    infoMap += ("positive_percent" -> positive/sum)
    val jsoninfo = JSONObject(infoMap).toString()
    println(classify +" " + infoMap("negative_percent") + " " + infoMap("positive_percent"))
    jsoninfo
  }

}
