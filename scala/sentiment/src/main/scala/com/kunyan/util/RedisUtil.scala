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
    * @return  返回redis资源
    * @author  liumiao
    */
  def getRedis: Jedis ={
    // 设置参数
    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)

    // 设置 redis 的 Host、port、password 和 database 等参数

    // 测试 --------------------------------------------------------------------------
    val redisHost = "222.73.57.12"
    val redisPort = 6379
    val redisTimeout = 30000
    val redisPassword = "kunyan"
    val redisDatabase = 0

    // 正式 --------------------------------------------------------------------------
//    val redisHost = "222.73.34.96"
//    val redisPort = 6390
//    val redisTimeout = 30000
//    val redisPassword = "7ifW4i@M"
//    val redisDatabase = 0

    // 链接数据库
    val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, redisPassword, redisDatabase)
    val redis = pool.getResource
    //  关闭 Pool
    pool.close()
    // 返回redis资源
    redis
  }

  /**
    * 写redis
    *
    * @param redis  redis资源
    * @param name  存储的表名
    * @param result  待存储序列
    * @author  liumiao
    */
  def writeToRedis(redis: Jedis, name:String, result:mutable.Map[String, String]): Unit ={
    for(i <- result){
      redis.hset(name, i._1, i._2)
    }
  }

}
