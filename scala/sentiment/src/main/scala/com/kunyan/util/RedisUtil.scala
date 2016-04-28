package com.kunyan.util

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
    * @return  返回jedis资源
    * @author  liumiao
    */
  def getRedis(sentimentConf :SentimentConf) : Jedis ={
    // 设置参数
    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)
    // 设置 redis 的 Host、port、password 和 database 等参数
    val redisHost = sentimentConf.getValue("redis", "ip")
    val redisPort = sentimentConf.getValue("redis", "port").toInt
    val redisTimeout = 30000
    val redisAuth = sentimentConf.getValue("redis", "auth")
    val redisDatabase = sentimentConf.getValue("redis", "db").toInt
    // 链接数据库
    val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, redisAuth, redisDatabase)
    val jedis = pool.getResource
    //  关闭 JedisPool
    pool.close()
    // 返回redis资源
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
    for(i <- result){
      jedis.hset(name, i._1, i._2)
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
