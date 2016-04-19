package com.kunyandata.nlpsuit.deduplication

import scala.collection.mutable.ArrayBuffer
import com.kunyandata.nlpsuit.util.TextPreprocessing

/**
  * Created by QQ on 2016/2/18.
  * 标题去重
  */

object TitleDeduplication {

  /**
    * 输入标题，返回一组哈希数组
    * @param titleString 标题字符串
    * @param n 字符串窗口大小，一般设置为2-4
    * @return 返回元素为哈希值的数组
    */
  private def hashList(titleString: String, n: Int) = {

      val title = TextPreprocessing.formatText(titleString)
      val titleList= new ArrayBuffer[Int]
      var loopCtrl = true
      for (w <- title if loopCtrl){
        val indexOfw = title.indexOf(w)
        val indexOfRange = indexOfw + n
        titleList.+=(title.slice(indexOfw, indexOfRange).hashCode)
        loopCtrl = indexOfw < (title.length - n)
      }
      titleList
    }

  /**
    * 对两个title字符串进行对比，返回是否疑似重复.
    * @param titleStringA 文本标题字符串（与titleStringB分别为两个需要比较的标题，交换顺序不影响结果）
    * @param titleStringB 文本标题字符串（与titleStringA分别为两个需要比较的标题，交换顺序不影响结果）
    * @param n 字符串窗口大小，建议设置2-4
    * @param threshold 判断是否相似的阈值，一般设置0.5-0.8（这个需要实际的检验）
    * @return 返回值为布尔值，true代表两个标题疑似重复，false反之
    */
  def process(titleStringA: String, titleStringB: String, n: Int, threshold: Double): Boolean = {

    val titleListA = hashList(titleStringA, n)
    val titleListB = hashList(titleStringB, n)
    val pValue = titleListA.intersect(titleListB).length*1.0/
      Array(titleListA.length, titleListB.length).max
    if (pValue >= threshold) true
    else false
  }

  def process(titleStringA: String, titleStringArray: Array[String], n: Int, threshold: Double): Boolean = {

    val titleListA = hashList(titleStringA, n)
    for (i <- titleStringArray.slice(0, -1)) {
      val titleListB = hashList(title, n)
    }
    val temp = titleStringArray.map(title => {

      val pValue = titleListA.intersect(titleListB).length * 1.0 /
        Array(titleListA.length, titleListB.length).max
      if (pValue >= threshold) true
      else false
    }).toSet
    temp.size
  }
}
