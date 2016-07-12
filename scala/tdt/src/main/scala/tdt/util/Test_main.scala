package com.kunyan.tdt.util

/**
  * Created by Administrator on 2016/7/11.
  */
object Test_main {

  class VertexState extends Serializable{

    var community = -1L   //所属社区ID
    var communitySigmaTot = 0L  //社区度数
    var internalWeight = 0L  //节点总度数
    var nodeWeight = 0L   //节点的出度
    var changed = false

    override def toString():String ={
      "{community:"+community+",communitySigmaTot:"+communitySigmaTot+"," +
        "internalWeight"+internalWeight+",nodeWeight"+nodeWeight+"}"
    }
  }

  var edgeRDD = sc.textFile(edgeFile).map(row => {
    val tokens =
  })
}
