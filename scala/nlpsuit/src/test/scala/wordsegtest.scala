import com.kunyandata.nlpsuit.util.TextPreprocessing

/**
  * Created by QQ on 4/29/16.
  */
object wordsegtest {
  def main(args: Array[String]) {
    println(TextPreprocessing.process(null, Array(""), 0).toSeq)

  }
}
