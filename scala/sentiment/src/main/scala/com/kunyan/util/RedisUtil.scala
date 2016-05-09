package com.kunyan.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by liumiao on 2016/4/13.
  * redis操作
  */

object RedisUtil {

  /**
    * 连接 redis
    * @param sentimentConf 配置文件
    * @return  redis连接
    * @author  liumiao
    */
  def getRedis(sentimentConf: SentimentConf): Jedis = {

    // 设置参数
    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)

    // 设置 redis 的 Host、port、auth 和 database 等参数
    val redisHost = sentimentConf.getValue("redis", "ip")
    val redisPort = sentimentConf.getValue("redis", "port").toInt
    val redisTimeout = 30000
    val redisAuth = sentimentConf.getValue("redis", "auth")
    val redisDatabase = sentimentConf.getValue("redis", "db").toInt
    val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, redisAuth, redisDatabase)
    val redis = pool.getResource
    pool.close()

    // 返回redis资源
    redis

  }

  /**
    * 写 redis
    * @param redis  redis连接
    * @param name  存储的表名
    * @param result  待存储序列
    * @author  liumiao
    */
  def writeToRedis(redis: Jedis, name: String, result: Map[String, String]): Unit = {

    for(i <- result){
      redis.hset(name, i._1, i._2)
    }

  }

}
