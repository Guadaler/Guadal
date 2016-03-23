package com.kunyandata.nlpsuit.util

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * Created by zx on 2016/3/22.
  */
object MySQLUtil {

  /**
    * 获取mysql数据库中的数据
    *
    * @param driver 注册driver
    * @param jdbcUrl jdbcurl
    * @param username 用户名
    * @param password 密码
    * @param sqlString sql搜索语句
    * @return 返回从数据库中读取的数据
    * @author LiYu
    */
  def GetResultSet(driver:String, jdbcUrl:String, username:String, password:String, sqlString:String): ResultSet = {
    var connection: Connection = null
    // 注册Driver
    Class.forName(driver)
    // 得到连接
    connection = DriverManager.getConnection(jdbcUrl, username, password)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sqlString)
    connection.close()
    resultSet
  }


}
