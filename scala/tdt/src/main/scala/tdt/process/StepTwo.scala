package com.kunyan.tdt.process

import com.kunyan.graph.louvain.{Louvain, LouvainConfig}
import com.kunyan.tdt.util.LoggerUtil
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Liu Miao on 2016/6/29.
  */
object StepTwo {

  /**
    * 初始化Louvain的config信息
    *
    * @param jsonConfig 配置文件
    * @return LouvainConfig信息文件
    */
  private def initLouvain(jsonConfig: JsonConfig): LouvainConfig = {

    val config = LouvainConfig(
      jsonConfig.getValue("tdt", "partition").toInt,
      jsonConfig.getValue("louvain", "minimumCompressionProgress").toInt,
      jsonConfig.getValue("louvain", "progressCounter").toInt,
      jsonConfig.getValue("louvain", "delimiter"),
      jsonConfig.getValue("tdt", "support").toInt
    )

    config
  }

  /**
    * 将数据转化为图
    * @param data 数据
    * @return
    */
  def getEdgeAloneRDD(data: RDD[((Long, Long), Long)]): RDD[Edge[Long]] = {

    val edgesRDD = data.map(row => {

      val edge1 = new Edge(row._1._1, row._1._2, row._2)
      val edge2 = new Edge(row._1._2, row._1._1, row._2)

      Array(edge1, edge2)
    }).flatMap(x => x)

    edgesRDD
  }

  /**
    * 社区发现主流程
    *
    * @param sc spark程序入口
    * @param edges 输入图的边
    * @param jsonConfig 配置文件
    * @return 通过筛选的社区
    */
  def runLouvain(sc: SparkContext,
                 edges: RDD[Edge[Long]],
                 jsonConfig: JsonConfig): mutable.Map[Int, RDD[(Long, String)]] = {

    val config = initLouvain(jsonConfig)

    // 获取社区成员数量的上下限
    val bottom = jsonConfig.getValue("louvain", "lower").toInt
    val top = jsonConfig.getValue("louvain", "upper").toInt

    val louvain = new Louvain
    val communities = louvain.run(sc, edges, config)

//    communities

    val newCommunities = mutable.Map[Int, RDD[(Long, String)]]()

    communities.foreach( level => {

      val filterCommunities = level._2.map(m => {

        val num = m._2.split("\t").length

        if (bottom <= num && num <= top ) {
          (m._1, m._2)
        }

      }).filter(_ != ()).map(_.asInstanceOf[(Long, String)])

      newCommunities += level._1 -> filterCommunities

    })

    newCommunities
  }

  /**
    * 将id映射为词
    *
    * @param community 社区（id）
    * @param dict 映射词典
    * @return 社区（词）
    */
  def idToWords(community: RDD[(Long, String)],
                dict: Map[Long, String]): RDD[(String, Array[String])] = {

    val wordsCommunity = community.map(c => {

      val words = new ArrayBuffer[String]()

      c._2.split("\t").foreach( id => {
        words.append(dict(id.toLong))
      })

      (dict(c._1.toLong), words.toArray)
    })

    wordsCommunity
  }

  def run(sc:SparkContext,
          distanceData: RDD[((Long, Long), (Int, Long))],
          dict: Map[String, Long],
          jsonConfig: JsonConfig) = {

    val graphCosine = StepTwo.getEdgeAloneRDD(distanceData.map(line => (line._1, line._2._2)))
    LoggerUtil.warn("构建图成功！ 》》》》》》》》》》》》》")

    val louvainCosine = StepTwo.runLouvain(sc, graphCosine, jsonConfig)
    LoggerUtil.warn("Louvain结束！ 》》》》》》》》》》》》》")

    val communityCosine = StepTwo.idToWords(louvainCosine(0), dict.map(line => (line._2, line._1))).collect()
    LoggerUtil.warn("映射完成！ 》》》》》》》》》》》》》")

    communityCosine
  }

}
