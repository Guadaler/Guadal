package com.kunyandata.nlpsuit.Statistic

import breeze.linalg.{sum, DenseVector}
import breeze.numerics.sqrt
import java.math.BigDecimal

import java.math.RoundingMode

/**
  * Created by QQ on 2016/5/6.
  * nlp基本统计方法
  */
object Similarity {

  /**
    * 设置小数点后几位，规则为四舍五入
    * @param num 数字
    * @param scale 需要保留的小数点位数
    * @return 数字
    */
  def setDotScale(num: Double, scale: Int): Double = {
    val temp = new BigDecimal(num)
    temp.setScale(scale, RoundingMode.HALF_UP).toString.toDouble
  }

  /**
    * 余弦距离计算（在breeze.linalg.function.cosineDistance方法中也实现了余弦距离求解，
    * 不过其确切的为角相似度，值为1 - cosineDistance）
    * @param x 待对比的向量
    * @param y 待对比的向量
    * @return 返回一个相似度的数值
    * @author QQ
    */
  def cosineDistance(x: DenseVector[Double], y: DenseVector[Double]): Double ={

    val dotProduct = sum(x :* y)
    val normProduct = sqrt(sum(x :* x)) * sqrt(sum(y :* y))

    dotProduct/normProduct
  }

}
