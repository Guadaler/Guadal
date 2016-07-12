package com.kunyan.tdt


import com.kunyan.tdt.process._
import com.kunyan.tdt.util.LoggerUtil
import com.kunyandata.nlpsuit.util.{JsonConfig, KunyanConf}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QQ on 6/27/16.
  */
object Main {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("warren_tdt")
//      .setMaster("local")
//      .set("spark.driver.host","192.168.2.90")

    val sc = new SparkContext(conf)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))

    val kunyanConf = new KunyanConf()
    kunyanConf.set(jsonConfig.getValue("kunyan", "ip"),
      jsonConfig.getValue("kunyan", "port").toInt)

    // 获取上一次热度计算结果
    val lastHotDegree = null
    // 李煜将你的获取热度计算的方法替换后面的null，数据格式为 Array[(String, Int)]

    // stepOne 获取数据，并清洗数据，返回(原始文本，词与id映射map，词对距离结果)
    val textData = StepOne.getAndFormatData(sc, jsonConfig, kunyanConf).cache()
    LoggerUtil.warn("数据条数：" + textData.count())
    LoggerUtil.warn("分词结束！ 》》》》》》》》》》》》》")

    // 构建词和ID的映射
    val dict = StepOne.makeDictsWithIndex(sc, textData)
    LoggerUtil.warn("词典长度：" + dict.size)
    LoggerUtil.warn("词典构建结束！ 》》》》》》》》》》》》》")

    // 计算词对间的距离
    val distanceData = StepOne.computeDistance(sc, textData, dict, jsonConfig).cache()
    LoggerUtil.warn("词对个数：" + distanceData.count())
    LoggerUtil.warn("距离计算结束！ 》》》》》》》》》》》》》")

    // stepTwo 用louvain聚类
    val communityCosine = StepTwo.run(sc, distanceData, dict, jsonConfig)


    val newCommunityCosine = communityCosine.map(community => {

      // 张鑫在这里添加计算subjectwords方法，输入为Array[String]，原始文本为textData，输出为String = subjectWord
      // 你的方法写在StepThree里，并封装为getSubjectWord，方法名字自己确定，这边只是示例，
      // 张鑫需要考虑的一个问题是，如果这个类和之前的类重复，但是提取的subjectWord不同应该如何处理？（事件发现Step3）
      val subjectWord = StepThree.getSubjectWord(community._2)
      LoggerUtil.warn(s"事件“${community._1}”，完成主题词抽取：$subjectWord！ 》》》》》》》》》》》》》")

      // 王草在这里添加方法，计算股票权重，输入为Array[String]，输出为String = stock:weight,stock:weight
      // 你的方法写入StepFour类中，并封装为getStockWeight，方法名字自己确定，这边只是示例
      val stockAndWeight = StepFour.getStockWeight
      LoggerUtil.warn(s"事件“${community._1}”，完成分类：$stockAndWeight！ 》》》》》》》》》》》》》")

      (subjectWord, community._2.mkString(","), stockAndWeight)
    })

    // 计算热度
    // 李煜在这里添加热度计算方法，输入为Array[(String, Array[String])]，原始文本为textData，
    // 上一小时计算结果为Array[(String, Double)]，输出为Array[(String, Double)]
    // 你的方法写在StepFive中，并且封装方法为computeHotDegree，方法名字自己确定，这边只是示例
    val hotDegree = StepFive.computeHotDegree
    LoggerUtil.warn("热度计算完成 》》》》》》》》》》》》》")

    // 以上工程你们只需要将自己的方法测试通过就行，整体工程的测试考虑到顺序，暂时不用测。

    // 合并结果，并存如数据库
    StepSix.run(newCommunityCosine, hotDegree)
    // 这里李煜需要提供写出hotDegree的方法

    sc.stop()
  }
}
