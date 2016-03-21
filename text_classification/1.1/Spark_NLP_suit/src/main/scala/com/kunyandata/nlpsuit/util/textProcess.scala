package com.kunyandata.nlpsuit.util

import java.io.StringWriter
import java.util

import org.apache.commons.io.IOUtils
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair

/**
  * Created by QQ on 2016/3/18.
  */
class textProcess {
   @native def write2proc(sentence: String): String
}


object textProcess{
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





















  def splitword(content: String, source: Int): String = {

    source match {
      case 0 =>
        val textProcess = new textProcess
        try {
          System.loadLibrary("WordSeg")
          toJson(textProcess.write2proc(content))
        } catch {
          case e: UnsatisfiedLinkError =>
            "Cannot load com.kunyandata.nlpsuit.util.WordSeg library:\n " + e.toString
        }

      case 1 =>

    }
  }


}
