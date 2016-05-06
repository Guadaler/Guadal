package com.kunyandata.nlpsuit.Statistic

import breeze.linalg._
import breeze.numerics.sqrt

/**
  * Created by QQ on 2016/5/6.
  * nlp基本统计方法
  */
object Similarity {

  /**
    *
    * @param x 待对比的向量
    * @param y 待对比的向量
    * @return 返回一个相似度的数值
    * @author QQ
    */
  def cosineDistance(x:DenseVector[Double], y:DenseVector[Double]): Double ={

    val dotProduct = sum(x :* y)
    val normProduct = sqrt(sum(x :* x)) * sqrt(sum(y :* y))

    dotProduct/normProduct

  }

//  /**
//    * 求和
//    *
//    * @param xs
//    * @return
//    */
//  def sum(xs: Array[Double]): Double =
//    if (xs.isEmpty) 0 else xs.head + sum(xs.tail)
}
