package com.kunyandata.newsparser.util

import java.io.{OutputStreamWriter, BufferedReader, InputStreamReader}
import java.text.SimpleDateFormat
import java.util

import com.kunyandata.newsparser.Scheduler
import com.kunyandata.newsparser.config.DataPathConfig
import com.kunyandata.newsparser.logger.NPLogger
import com.kunyandata.newsparser.model.NewsItem
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject


/**
  * Created by QQ on 2016/1/16.
  * @author qiuqiu
  */
object DataUtil {

  def getDate: Map[String, String] = {
    val timeFormat = new SimpleDateFormat("HHmmss")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val timestringFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = dateFormat.format(System.currentTimeMillis())
    val time = timeFormat.format(System.currentTimeMillis())
    val timestring = timestringFormat.format(System.currentTimeMillis())
    val result = Map("date" -> date, "time" -> time, "timestring" -> timestring)
    result
  }

  def NewsItemToJson(row: NewsItem, timestring: String, id: String, attr: String): String = {
    var rowMap: Predef.Map[String, Any] = Predef.Map()
    rowMap += ("title" -> row.title)
    rowMap += ("up" -> 0)
    rowMap += ("down" -> 0)
    rowMap += ("indus" -> row.industry)
    rowMap += ("sect" -> row.section)
    rowMap += ("url" -> row.url)
    rowMap += ("stock" -> row.category)
    rowMap += ("time" -> timestring)
    rowMap += ("id" -> id)
    rowMap += ("from" -> attr)
    val jsonResult = JSONObject(rowMap).toString()
    jsonResult
  }

  def loadDataToRedis(newsItem: NewsItem, industryDict: Map[String, Array[String]], stockDict: Map[String, Array[String]], sectionDict: Map[String, Array[String]]): Unit = {

    val platform = Scheduler.hashMapObtainNameAndId.get(newsItem.platformId)

    //    将文章信息写入redis
    val pj = Scheduler.jedis
    if (!pj.isConnected) {
      NPLogger.error("Unconnected")
      pj.connect()
    }

    val dateHour = getDate
    val name = "News_" + dateHour("date")
    val urlMd5 = DigestUtils.md5Hex(newsItem.url)
    val id = urlMd5 + "_" + dateHour("time")
    val value = NewsItemToJson(newsItem, dateHour("timestring"), id, platform)
    pj.hset(name, id, value)
    pj.expire(name, 60 * 60 * 48)
    //    将文章类别信息分别写入redis中
    //    NPLogger.warn("Industry: " + row.industry + " Stock: " + row.category + " Section: " + row.section)
    val cate = Map("Industry" -> newsItem.industry, "Stock" -> newsItem.category, "Section" -> newsItem.section)
    for (cat <- cate.keys) {
      if (cat == "Industry") {
        for (indus: String <- industryDict.keys) {
          val temps = newsItem.industry.split(",")
          if (temps.contains(indus)) {
            val name_indus = cat + "_" + dateHour("date")
            val oldValue = pj.hget(name_indus, indus)
            if (oldValue != null) {
              val newValue = oldValue + "," + id
              pj.hset(name_indus, indus, newValue)
              pj.expire(name_indus, 60 * 60 * 48)
            } else {
              pj.hset(name_indus, indus, id)
              pj.expire(name_indus, 60 * 60 * 48)
            }
          }
        }
      } else if (cat == "Stock") {
        for (stock: String <- stockDict.keys) {
          val temps = newsItem.category.split(",")
          if (temps.contains(stock)) {
            val name_stock = cat + "_" + dateHour("date")
            val oldValue = pj.hget(name_stock, stock)
            if (oldValue == null) {
              pj.hset(name_stock, stock, id)
              pj.expire(name_stock, 60 * 60 * 48)
            } else {
              val newValue = oldValue + "," + id
              pj.hset(name_stock, stock, newValue)
              pj.expire(name_stock, 60 * 60 * 48)
            }
          }
        }
      } else {

        try {
          for (sect: String <- sectionDict.keys) {
            val temps = newsItem.section.split(",")
            if (temps.contains(sect)) {
              val name_sect = cat + "_" + dateHour("date")
              val oldValue = pj.hget(name_sect, sect)
              if (oldValue == null) {
                pj.hset(name_sect, sect, id)
                pj.expire(name_sect, 60 * 60 * 48)
              } else {
                val newValue = oldValue + "," + id
                pj.hset(name_sect, sect, newValue)
                pj.expire(name_sect, 60 * 60 * 48)
              }
            }
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
  }

  def formatDict(string: String): (String, Array[String]) = {
    //    val resultTemp = new mutable.HashMap[String, Array[String]]
    val row = string
    val rowSeg = row.split("\t")
    val rowWordSeg = rowSeg(1).split(",")
    rowSeg(0) -> rowWordSeg
  }

  /**
    * 将HFDS上的配置文件内容转换为map
    * @author yangshuai
    */
  def getMapFromHDFS(path: String): Map[String, Array[String]] = {

    val map = new mutable.HashMap[String, Array[String]]()

    val pt = new Path(path)
    val fs = FileSystem.get(pt.toUri, new Configuration())
    val br = new BufferedReader(new InputStreamReader(fs.open(pt)))
    var line = br.readLine()

    while (line != null) {
      map.+=(formatDict(line))
      line = br.readLine
    }

    br.close()
    fs.close()
    map
  }

  /**
    * 返回hashMap id和网页name对应的hashMap
    **/
  def getDataNameByIdFromHDFS(path: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    val pt = new Path(path)
    val fs = FileSystem.get(pt.toUri, new Configuration())
    val br = new BufferedReader(new InputStreamReader(fs.open(pt), "GBK"))
    var line = br.readLine()

    while (line != null) {
      val result = formatIdAndName(line)
      if (result != null) {
        map.put(result._1, result._2)
      }
      line = br.readLine()
    }

    br.close()
    fs.close()
    map
  }

  def formatIdAndName(string: String): (String, String) = {
    if (string.trim != "" || string.trim != null) {
      val row = string
      val rowSeg = row.split(",")
      return (rowSeg(0), rowSeg(1))

    }
    null
  }

}
