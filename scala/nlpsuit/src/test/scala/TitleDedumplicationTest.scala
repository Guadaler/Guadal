/**
  * Created by QQ on 2016/2/19.
  */


import java.util.Date

import org.scalatest.{FlatSpec, Matchers}
import com.kunyandata.nlpsuit.deduplication.TitleDeduplication

/**
  * Created by QQ on 2016/2/21.
  */
class TitleDedumplicationTest extends  FlatSpec with Matchers{

  "test " should "work" in{

    val n = 2
    val title1 = "远望谷涨停 将独家供应上海迪士尼梦想护照"
    val title2 = "远望谷涨停 将独家供应迪士尼梦想护照"
    val time1 = new Date().getTime
    for (i <- 1 to 5000){
      TitleDeduplication.process(title1, title2, n, 0.4)
    }
    val time2 = new Date().getTime
    print(time2 - time1)



//    for (w: Char <- title) {
//      println(w)
//    }
  }
}
