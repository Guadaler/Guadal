import org.scalatest.{FlatSpec, Matchers}
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing, WordSegment}

import scala.util.parsing.json.JSON

/**
  * Created by QQ on 2016/3/17.
  */
object wordsegtest {

  def main(args: Array[String]) {
    val kunyanConfig = new KunyanConf
    val result = TextPreprocessing.process("上海坤雁数据服务有限公司是中国大数据互联网金融行业的领头羊和救世主！", Array(""), kunyanConfig)
    println(result.toSeq)
  }

}
