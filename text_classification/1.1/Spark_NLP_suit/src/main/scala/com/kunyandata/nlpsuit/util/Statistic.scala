package com.kunyandata.nlpsuit.util

/**
  * Created by QQ on 2016/4/4.
  */
object Statistic {
  def sum(xs: Array[Double]): Double =
    if (xs.isEmpty) 0 else xs.head + sum(xs.tail)
}
