package com.kunyandata.nlpsuit.util

import java.sql.{DriverManager, Connection}

/**
  * Created by zx on 2016/3/22.
  */
class MySQLUtil {
  def main(args: Array[String]) {
    var sqlstr="SELECT id, title FROM indus_text_with_label WHERE id<=1000"
    var connection=getConn();
    //    val statement = connection.createStatement();
    //    val resultSet = statement.executeQuery(sqlstr)
    //    var count=0
    //    while ( resultSet.next() ) {
    //      count +=1
    //      val id = resultSet.getString("id")
    //      val title = resultSet.getString("title")
    //      println("id:" + id + ",  title =  " + title)
    //    }
    //    println("总共取出："+count+" 条数据")
    //    connection.close();

    var result=getNews(connection)
    for(news <-result.keys){
      println(news+"  :"+result.get(news).get.trim())
    }
  }

  /**
    * 获取MySQL连接
    *
    * @return  connection
    */
  def getConn(): Connection ={
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.1.14:3306/stock"
    val username = "root"
    val password = "root"
    var connection:Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    }catch {
      case e => e.printStackTrace
    }
    connection
  }

  /**
    * 取新闻
    * @param conn
    * @return   新闻map[title,content]
    */
  def getNews(conn:Connection):Map[String,String] ={
    var count=1;
    var count_cf=0;
    var result=Map[String,String]();
    var sqlstr="SELECT title,content FROM indus_text_with_label WHERE id>3000 and id<=15000"
    val statement = conn.createStatement();
    val resultSet = statement.executeQuery(sqlstr)
    while ( resultSet.next() ) {
      count +=1;
      val title= resultSet.getString("title").trim();
      val content= resultSet.getString("content").trim();
      result +=(title -> content);
    }
    result
  }
}
