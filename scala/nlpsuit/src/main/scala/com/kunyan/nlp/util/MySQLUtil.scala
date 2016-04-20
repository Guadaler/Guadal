package com.kunyan.nlp.util

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * Created by zx on 2016/3/22.
  */
object MySQLUtil {

  /**
    * 建立连接
    * @param driver 注册driver
    * @param jdbcUrl jdbcurl
    * @param username 用户名
    * @param password 密码
    * @return 返回从数据库中读取的数据
    * @author LiYu
    */
  def getConnect(driver:String, jdbcUrl:String, username:String, password:String ): Connection = {
    var connection: Connection = null
    try {
      // 注册Driver
      Class.forName(driver)
      // 得到连接
      connection = DriverManager.getConnection(jdbcUrl, username, password)
      //    connection.close()
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    connection
  }


  /**
    * 获取mysql数据库中的数据
    * @param sqlString 注册driver
    * @param connection jdbcurl
    * @return 返回从数据库中读取的数据
    * @author LiYu
    */
  def getResult(connection:Connection, sqlString:String) :ResultSet={
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sqlString)
    resultSet
  }

  /**
    * 按照ID区间取新闻,并封装到Map[title,content]
    * @param conn  数据库连接
    * @param idBegin 起始ID
    * @param idEnd 终止ID
    * @return 新闻Map
    * @author zhangxin
    */
  def getNews(conn:Connection,idBegin:Int,idEnd:Int):Map[String,String] ={
    var result=Map[String,String]()
    val sqlstr="SELECT title,content FROM indus_text_with_label WHERE id>"+idBegin+"and id<="+idEnd
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(sqlstr)
    while ( resultSet.next() ) {
      val title= resultSet.getString("title").trim()
      val content= resultSet.getString("content").trim()
      result +=(title -> content)
    }
    result
  }

}
