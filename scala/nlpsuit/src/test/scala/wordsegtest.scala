import org.scalatest.{FlatSpec, Matchers}
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing, WordSegment}

import scala.util.parsing.json.JSON

/**
  * Created by QQ on 2016/3/17.
  */
object wordsegtest {

  def main(args: Array[String]) {
    val kunyanConfig = new KunyanConf
    kunyanConfig.set("222.73.57.17", 16003)
    val result = TextPreprocessing.process("上海坤雁数据服务有限公司是中国大数据互联网金融行业的领头羊和救世主！", Array("上海"), kunyanConfig)
    val result2 = TextPreprocessing.process("上海坤雁数据服务有限公司是中国大数据互联网金融行业的领头羊和救世主！", Array("上海"), kunyanConfig)
    println(result.toSeq)
    println(result2.toSeq)
  }

}
