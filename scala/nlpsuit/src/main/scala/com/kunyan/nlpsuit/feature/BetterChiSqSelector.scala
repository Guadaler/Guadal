package com.kunyan.nlpsuit.feature

/**
  * Created by QQ on 2016/4/6.
  */

import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD

class BetterChiSqSelector(){

  def preFit(data: RDD[LabeledPoint]): Array[(ChiSqTestResult, Int)] = {
    val indices = Statistics.chiSqTest(data)
      .zipWithIndex.sortBy { case (res, _) => -res.statistic }
    indices
  }

  def fit(temp: Array[(ChiSqTestResult, Int)], numTopFeatures: Int): ChiSqSelectorModel = {
    val result = temp
      .take(numTopFeatures)
      .map { case (_, indices) => indices }
      .sorted
    new ChiSqSelectorModel(result)
  }
}
