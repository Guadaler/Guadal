package com.kunyandata.nlpsuit.util

/**
  * Created by QQ on 2016/2/18.
  */

import java.io.StringWriter
import java.util
import org.apache.commons.io.IOUtils
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import scala.util.parsing.json.JSON

class WordSeg {
  @native def splitSentence(sentence: String): String
  @native def start(segAppPath: String): Int
  @native def stop(): Unit
}

object WordSeg {

  val LOCAL = 0
  val REMOTE = 1

  def main(args: Array[String]) {
    println(splitWord("我爱北京天安门，天安门上台养生", "/home/mlearning/bin/", this.LOCAL))
  }

  /**
    *
    * @param content 需要分词的字符串
    * @param source 0:使用本地的so文件, 1:通过http调用分词API
    * @return
    */
  def splitWord(content: String, path:String, source: Int): String = {

    source match {
      case 0 =>
        val wordSeg = new WordSeg

        try {
          System.loadLibrary("WordSeg")
          wordSeg.start(path)
          val result = toJson(wordSeg.splitSentence(content + "\n"))
          wordSeg.stop()
          result
        } catch {
          case e: UnsatisfiedLinkError =>
            "Cannot load com.kunyandata.nlpsuit.util.WordSeg library:\n " + e.toString
        }

      case 1 =>
        sendPost(content)
    }
  }

  /**
    * 将本地分词结果转换成json格式
    *
    * @param str 分词结果
    * @return json
    */
  private def toJson(str: String): String = {
    val arr = str.split("\t")
    var json = "{\"result\": {\"segment\":["
    for (i <- 0 until (arr.length - 1) / 3) {
      json += String.format("{\"word\":\"%s\",\"type\":\"%s\",\"idf\":\"%s\"},", arr(i * 3), arr(i * 3 + 1), arr(i * 3 + 2))
    }

    if (arr.nonEmpty) {
      json = json.substring(0, json.length - 1)
    }

    json + String.format("],\"idf\":\"%s\"}}", arr.last.replaceAll("\n", ""))
  }

  /**
    * 通过网络接口获得分词结果
    *
    * @param content 需要分词的字符串
    * @return json格式的分词结果
    */
  private def sendPost(content: String): String = {

    val url = "http://112.124.49.59/cgi-bin/nlp/segment/v1/segment_word.fcgi"
    val httpclient = HttpClients.createDefault()
    val httpPost = new HttpPost(url)

    // Request parameters and other properties.
    val params = new util.ArrayList[NameValuePair]()

    params.add(new BasicNameValuePair("uid", "100001"))
    params.add(new BasicNameValuePair("token", "qR3E1122SDD8B31EFBBD"))
    params.add(new BasicNameValuePair("content", content))
    httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"))
    httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded")


    var result = ""
    //Execute and get the response.
    val response = httpclient.execute(httpPost)
    val entity = response.getEntity

    if (entity != null) {
      val inputStream = entity.getContent
      try {
        val writer = new StringWriter()
        IOUtils.copy(inputStream, writer, "utf-8")
        result = writer.toString
      } catch {
        case e: Exception =>
          e.printStackTrace()
          result = "{\"status\":0}"
      } finally {
        inputStream.close()
      }
      result
    } else {
      "{\"status\":0}"
    }
  }

  /**
    *
    * @param json 坤雁分词的结果
    * @return 返回一个由词组成的数组
    */
  def getWords(json: String): Array[String] = {

    try{
      val jsonResult = JSON.parseFull(json).get.asInstanceOf[Map[String, Any]]
      val resultTemp = jsonResult("result")
        .asInstanceOf[Map[String, Any]]("segment")
        .asInstanceOf[List[Map[String, String]]]
      val resultTmp = resultTemp.map(line => {
        line("word")
      }).toArray
      resultTmp
    }catch {
      case e:Exception => e.printStackTrace()
        null
    }
  }
}