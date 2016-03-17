import org.scalatest.{Matchers, FlatSpec}
import com.kunyandata.nlpsuit.util.WordSeg
import scala.util.parsing.json.JSON

/**
  * Created by QQ on 2016/3/17.
  */
class WordSegTest extends  FlatSpec with Matchers {
  "test " should "work" in{
    val result = WordSeg.splitWord("我爱你中国，你是我的母亲！", 1)
    println(result)
    val jsonResult = JSON.parseFull(result)
    val result1 = jsonResult.get.asInstanceOf[Map[String, Any]]
    val a = result1("result")
      .asInstanceOf[Map[String, Any]]("segment")
      .asInstanceOf[List[Map[String, String]]]
    val c = a.map(line => {
      line("word")
    }).toArray
    c.foreach(println)
  }
}
