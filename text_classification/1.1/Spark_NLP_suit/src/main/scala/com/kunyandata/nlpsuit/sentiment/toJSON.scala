package com.kunyandata.nlpsuit.sentiment

import scala.util.parsing.json.JSONObject

/**
  * Created by Administrator on 2016/2/25.
  */
object toJSON {

  def toJSON(classify:String, p:Int, n:Int, m:Int, sum:Float):String = {
    var infoMap:Predef.Map[String, Float] = Predef.Map()
    infoMap += ("positive_percent" -> p/sum)
    infoMap += ("negative_percent" -> n/sum)
    infoMap += ("neutral_percent" -> m/sum)
    val jsoninfo = JSONObject(infoMap).toString()

    println(classify + " " + infoMap("positive_percent") + " " + infoMap("negative_percent") + " " + infoMap("neutral_percent"))

    jsoninfo

  }

}


