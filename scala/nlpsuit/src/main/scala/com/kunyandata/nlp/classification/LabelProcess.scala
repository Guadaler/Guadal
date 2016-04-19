package com.kunyandata.nlp.classification

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by QQ on 2016/3/23.
  */
object LabelProcess extends App{

//  val mySqlConn = MySQLUtil.getConnect("com.mysql.jdbc.Driver",
//    "jdbc:mysql://192.168.1.14:3306/stock", "root", "root")
//  val resultSet = MySQLUtil.getResult(mySqlConn, "select url, indus_code from indus_text_with_label")
//  val oldIndus = mutable.Map[String, ArrayBuffer[String]]()
//  while(resultSet.next()){
//    val url = resultSet.getString("url").trim
//    val indus_code = resultSet.getString("indus_code").trim
//    if(oldIndus.keys.toArray.contains(url)){
//      oldIndus(url).append(indus_code)
//    }else{
//      oldIndus += (url -> ArrayBuffer(indus_code))
//    }
//  }
//  mySqlConn.close()
//
////   获取同花顺indus_code和行业名之间的对应关系
//  val thsIndusName = Source.fromFile("D:/mlearning/thsIndus").getLines().toArray
//  val thsIndusNameMap = thsIndusName.map(line => {
//    val temp = line.split("\t")
//    (temp(0), temp(1))
//  }).toMap
//
//  val oldIndusName = oldIndus.map(line => {
//    val temp = line._2.map(indusCode => {
//      thsIndusNameMap(indusCode)
//    }).toSeq
////    println((line._1, temp))
//    (line._1, temp)
//  }).toMap
//
//  val DataFile = new File("D:/mlearning/trainingLabel.old")
//  val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
//  oldIndusName.foreach(x => {
//    bufferWriter.write(x._1 + "\t" + x._2.mkString(",") + "\n")
//  })
//  bufferWriter.flush()
//  bufferWriter.close()
//  oldIndusName.foreach(println)

  val trainingLabelOld = Source.fromFile("D:/mlearning/trainingLabel.old").getLines().toArray
  val newIndusNameToOld = Source.fromFile("D:/mlearning/dzhToths").getLines().toArray
  val newIndusNameArray = newIndusNameToOld.map(line => {
    val temp = line.split("\t")
    val tmp = temp(1).split(",").toSeq
    (temp(0), tmp)
  })

  val result = trainingLabelOld.map(line => {
    val newCate = new ArrayBuffer[String]
    val tuple = line.split("\t")
    val oldLabel = tuple(1).split(",")
    newIndusNameArray.foreach(line => {
      line._2.foreach(oldName => {
        if(oldLabel.contains(oldName)){
          newCate.append(line._1)
        }
      })
    })
    (tuple(0), newCate.toSet.toArray)
  })

  val DataFile = new File("D:/mlearning/trainingLabel.new")
  val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
  result.foreach(x => {
    bufferWriter.write(x._1 + "\t" + x._2.mkString(",") + "\n")
  })
  bufferWriter.flush()
  bufferWriter.close()
}
