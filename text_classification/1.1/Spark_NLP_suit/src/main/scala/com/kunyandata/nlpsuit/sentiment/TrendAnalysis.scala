package com.kunyandata.nlpsuit.sentiment

/**
  * Created by QQ on 2016/2/23.
  * 情感倾向分析
  */
object TrendAnalysis {

  /**
    * 判断情感词前面是否有否定词
    *
    * @param i 情感词在标题中的位置
    * @param sentence 标题的分词结果
    * @param dic 否定词词典
    * @return 返回-1代表有否定词，返回1代表没有否定词
    */
  private def negWords(i:Int, sentence:Array[String], dic:Array[String]): Int ={
    /* this will search neg word nearby the sentiment word
     * if find a neg word, reverse the sentiment word's emotional tendency
     */
    // find neg word before sentiment word
    if (i-1 > 0){
      if (dic.contains(sentence(i-1))){
        return -1
      }
      else if (i-2 >0){
        if (dic.contains(sentence(i-2))){
          return  -1
        }
      }
    }
    // fine neg word behind sentiment word
    if (i+1 < sentence.length){
      if(dic.contains(sentence(i+1))){
        return -1
      }
      else if(i+2 < sentence.length){
        if (dic.contains(sentence(i+2))){
          return -1
        }
      }
    }
    // with no neg word return 1
    1
  }

  /**
    * 情感倾向判别
    *
    * @param title_cut 分词后的标题
    * @param dict_p 正向情感词典
    * @param dict_n 负向情感词典
    * @param dict_f 否定词词典
    * @return -1,0,1分别代表负向，中性，正向情感的判别结果
    */
  def sentiDiscriminant(title_cut:Array[String], dict_p:Array[String],
                        dict_n:Array[String], dict_f:Array[String]): Int ={
    /* this will search the sentiment words in the sentence
     * count the sentence emotional tendency according to the sentiment word
     */
    var p = 0
    var n = 0

    // traverse every word in sentence
    for (i <- title_cut.indices) {
      val t_c = title_cut(i)

      // if word in positive dictionary
      if(dict_p.contains(t_c)){
        if(negWords(i, title_cut, dict_f)>0){
          p = p + 1
        }
        else{
          n = n + 1
        }
      }

      // if word in negative dictionary
      else if (dict_n.contains(t_c)){
        if(negWords(i, title_cut, dict_f)>0){
          n = n + 1
        }
        else{
          p = p + 1
        }
      }
    }
    // positive
    if (p > n){
      1
    }
    // negative
    else if (p < n){
      -1
    }
    // neutral
    else{
      0
    }
  }

}
