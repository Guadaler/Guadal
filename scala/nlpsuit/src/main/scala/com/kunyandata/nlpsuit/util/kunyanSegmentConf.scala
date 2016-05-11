package com.kunyandata.nlpsuit.util

/**
  * Created by QQ on 2016/5/11.
  */
class KunyanConf {

  var port: Int = 0

  var host: String = null

  /**
    * 设置坤雁服务的ip和端口
    * @param host kunyan分词器服务的地址
    * @param port kunyan分词器服务地址的port
    */
  def set(host: String, port: Int) = {
    this.host = host
    this.port = port
  }
}
