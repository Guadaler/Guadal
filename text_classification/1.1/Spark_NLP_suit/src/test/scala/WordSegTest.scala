import org.scalatest.{Matchers, FlatSpec}
import com.kunyandata.nlpsuit.util.WordSeg

import scala.util.parsing.json.JSON

/**
  * Created by QQ on 2016/3/17.
  */
class WordSegTest extends  FlatSpec with Matchers {
  "test " should "work" in{
    val result = WordSeg.splitWord("上海坤雁数据服务有限公司是中国大数据互联网金融行业的领头羊和救世主！", 1)
    println(result)
    val jsonResult = JSON.parseFull(result)
    val result1 = jsonResult.get.asInstanceOf[Map[String, Any]]
    val a = result1("result")
      .asInstanceOf[Map[String, Any]]("segment")
      .asInstanceOf[List[Map[String, String]]]
    val c = a.map(line => {
      line("word")
    }).toSeq
    println(c)
  }
}
