/**
  * Created by QQ on 2016/2/19.
  */


import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/2/21.
  */
class TitleDedumplicationTest extends  FlatSpec with Matchers{

  "test " should "work" in{

    def hashList(titleString: String, n: Int) = {

      val title = formatTitle(titleString)
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

    def formatTitle(titleString: String): String = {
      val engPunc = Array(",", ".", "!", ";", ":","\"","\"")
      val chiPunc = Array("，", "。", "！", "；", "：", "“", "”")
      val blank = """\s"""
      var result = titleString.toLowerCase().replaceAll(blank, "")
      for (ind <- chiPunc) {
        result = result.replaceAll(ind, engPunc(chiPunc.indexOf(ind)))
      }
      result
    }

    val n = 2
    val title1 = "远望谷涨停 将独家供应上海迪士尼梦想护照"
    val title2 = "远望谷涨停 将独家供应迪士尼梦想护照"

    val title3 = "我爱Beijing，      天安门"
    println(formatTitle(title1))
    println(formatTitle(title2))

    val titleListA = hashList(title1, n)
    val titleListB = hashList(title2, n)
    println(titleListA)
    println(titleListB)
    val pValue = titleListA.intersect(titleListB).length*1.0/
      Array(titleListA.length, titleListB.length).max

    println(pValue)
    println(formatTitle(title3))
    println(title1.replaceAll("""\s""", ""))




//    for (w: Char <- title) {
//      println(w)
//    }
  }
}
