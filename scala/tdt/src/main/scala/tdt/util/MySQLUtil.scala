package com.kunyan.tdt.util

import java.sql.{Connection, DriverManager}

/**
  * Created by liumiao on 2016/6/24.
  * 数据库操作
  */
object MySQLUtil {

  /**
    * 连接MySQL
    * @param jdbcUrl 数据库信息
    * @return 数据库连接
    * @author liumiao
    */
  def getConnect(jdbcUrl: String): Connection = {

    var connection: Connection = null

    try {
      Class.forName("com.mysql.jdbc.Driver")   // Load the driver
      connection = DriverManager.getConnection(jdbcUrl)  // Setup the connection
    }
    catch {
      case e:Exception => e.printStackTrace()
    }

    connection
  }

  /**
    * 创建表
    * @param conn 数据库连接
    * @param table 表名
    * @author liumiao
    */
  def createTable(conn: Connection, table:String): Unit = {

    val prep0 = conn.createStatement()
    prep0.execute("CREATE TABLE IF NOT EXISTS " + table +
      " (community TEXT, vertices LONGTEXT, time BIGINT(20)); ")

  }

  /**
    * 写MySQL
    * @param result 写入的信息
    * @author liumiao
    */
  def writeMySQL(mysqlUrl: String, result: Map[String, String]): Unit = {

    val conn = getConnect(mysqlUrl)

    val updateSql = "INSERT INTO events (community, vertices, time) VALUES (?, ?, ?)"

    // do database insert
    val prep = conn.prepareStatement(updateSql)
    val time = System.currentTimeMillis()

    result.foreach(row => {

      prep.setString(1, row._1)
      prep.setString(2, row._2)
      prep.setLong(3, time)
      prep.executeUpdate

    })

  }

}
