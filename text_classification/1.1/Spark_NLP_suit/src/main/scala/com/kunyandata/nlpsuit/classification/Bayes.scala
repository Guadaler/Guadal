package com.kunyandata.nlpsuit.classification

/**
  * Created by QQ on 2016/2/18.
  */

import com.kunyandata.nlpsuit.util.WordSeg
import org.apache.spark.mllib.classification.NaiveBayesModel

object Bayes {


  def predict(content: String) = {
    val wordSegJson = WordSeg.splitWord(content, 1)
    val wordSeg = WordSeg.getWords(wordSegJson)

  }
}
