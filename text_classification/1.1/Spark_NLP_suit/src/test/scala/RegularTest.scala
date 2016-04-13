/**
  * Created by QQ on 2016/2/19.
  */


import org.scalatest.{Matchers, FlatSpec}
import scala.collection.{mutable, Map}

class RegularTest extends  FlatSpec with Matchers{

  "test " should "work" in{

    def grep(textString: String, categoryKeywords: Map[String, Array[String]],
                     categoryList: mutable.MutableList[String]): Unit = {

      for (indus: String <- categoryKeywords.keys) {
        var i_control = true
        for (keyword: String <- categoryKeywords(indus) if i_control) {
          val exists = textString.contains(keyword)
          if (exists) {
            categoryList.+=(indus)
            i_control = false
          }
        }
      }
    }

    def predict(textString: String, stockDict: Map[String, Array[String]],
                indusDict: Map[String, Array[String]],
                sectDict: Map[String, Array[String]]) = {

      val industryList = new mutable.MutableList[String]
      val stockList = new mutable.MutableList[String]
      val sectionList = new mutable.MutableList[String]

      //    行业分类
      grep(textString, indusDict, industryList)
      //    概念板块分类
      grep(textString, sectDict, sectionList)
      //    股票分类
      grep(textString, stockDict, stockList)

      industryList.mkString(",")
      sectionList.mkString(",")
      stockList.mkString(",")

      //    返回值的顺序为股票，行业，版块
      (stockList.mkString(","), industryList.mkString(","), sectionList.mkString(","))
    }

    val title = "我爱北京天安门，天安门上太阳升"
    val dict = Map("a" -> Array("北京", "太阳"), "b" -> Array("天", "门"), "c" -> Array("伤害", "上海"))
    val x = predict(title, dict, dict, dict)
    println(x)
    x.productIterator.foreach(println)
  }
}