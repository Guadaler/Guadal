/**
  * Created by QQ on 4/28/16.
  */
import com.kunyandata.nlpsuit.util.{TextPreprocessing, WordSeg}

import scala.io.Source

object wordSegTest {
  def main(args: Array[String]): Unit = {
    val stopWords = Source.fromFile("/home/QQ/mlearning/dicts/stop_words_CN").getLines().toArray
    println(WordSeg.splitWord("互联网金融难救主 沪指尾盘小幅放量下杀 不能轻易说不", TextPreprocessing.getKunyanPath, WordSeg.LOCAL))
    val result = TextPreprocessing.process("互联网金融难救主 沪指尾盘小幅放量下杀 不能轻易说不", stopWords, WordSeg.LOCAL)
    println(result.toSeq)
  }
}
