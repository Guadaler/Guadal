package com.kunyandata.nlpsuit.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by QQ on 2016/3/23.
  */
object LabelProcess extends App{

  val mySqlConn = MySQLUtil.getConnect("com.mysql.jdbc.Driver",
    "jdbc:mysql://192.168.1.14:3306/stock", "root", "root")
  val resultSet = MySQLUtil.getResult(mySqlConn, "select url, indus_code from indus_text_with_label")
  val oldIndus = mutable.Map[String, ArrayBuffer[String]]()
  while(resultSet.next()){
    val url = resultSet.getString("url").trim
    val indus_code = resultSet.getString("indus_code").trim
    if(oldIndus.keys.toArray.contains(url)){
      oldIndus(url).append(indus_code)
    }else{
      oldIndus += (url -> ArrayBuffer(indus_code))
    }
  }
  mySqlConn.close()

  // 获取同花顺indus_code和行业名之间的对应关系
  val thsIndusName = Source.fromFile("D:/mlearning/thsIndus").getLines().toArray
  val thsIndusNameMap = thsIndusName.map(line => {
    val temp = line.split("\t")
    (temp(0), temp(1))
  }).toMap

  val oldIndusName = oldIndus.map(line => {
    val temp = line._2.map(indusCode => {
      thsIndusNameMap(indusCode)
    }).toSeq
    println((line._1, temp))
    (line._1, temp)
  }).toMap
//  oldIndusName.foreach(println)

//  val newIndusNameToOld = Source.fromFile("D:/dzhToths").getLines().toArray
//  val newIndusNameArray = newIndusNameToOld.map(line => {
//    val temp = line.split("\t")
//    val tmp = temp(1).split(",")
//    (temp(0), tmp)
//  })
//
//  val newCate = new ArrayBuffer[String]
//  val result = oldIndusName.map(tuple => {
//    newIndusNameArray.foreach(line => {
//      line._2.foreach(oldName => {
//        if(tuple._2.contains(oldName)) newCate.append(line._1)
//      })
//    })
//    println((tuple._1, newCate))
//    (tuple._1, newCate)
//  })
}
