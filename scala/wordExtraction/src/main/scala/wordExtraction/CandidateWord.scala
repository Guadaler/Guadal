package wordExtraction

import com.kunyandata.nlpsuit.util.AnsjAnalyzer

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangxin on 2016/6/16.
  *
  * 提取候选关键词
  *
  * 依据词性模式，主要提起名词性短语作为候选关键词
  */
object CandidateWord {
  def main(args: Array[String]) {
    val content = "《爱情公寓》系列曾是“假期金牌节目”！没看到《爱情公寓》就如同没有放假一样，已经大红大紫的四季让人也无限期待第五部的开拍。" +
      "可是天意弄人，就在眼看《爱5》要来之际，王牌曾小贤的扮演者陈赫却发生了离婚，出轨等负面新闻，所以《爱5》的拍摄以及人员一直是人们的关注点。"

    val candiWordTest=candiWord(content)
    candiWordTest.foreach(line =>println(line._1+line._2))

  }

  /**
    * 带词性的词项去停
    * @param content 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWordsWithPOS(content: Array[String], stopWords:Array[String]): Array[String] = {
    var result=content.toBuffer
    if (result !=null) {
      result.foreach(line=>{
        val temp=if(line!=null) line.split("/") else Array("")
        if(temp.length>1){
          val word=temp(0)
          if(stopWords.contains(word)){
            result = result.filterNot(_ == line)
          }
        }
      })
      result.toArray
    } else {
      null
    }
  }


  /**
    * 提取候选关键词
    * @param content 文本
    * @return 候选关键词数组
    */
  def candiWord(content:String): Array[(String,String)] ={

    //切词  Ansj
    val contentSegTerm = AnsjAnalyzer.Nlp_cut(content).filterNot(!_.toString.contains("/"))
    val contentSegString = contentSegTerm.map(_.toString.trim)

    //提取
    val candiWord = new ArrayBuffer[(String,String)]()
    val cadiWordCopy = new Array[String](2)

    for (n <- Range(0,contentSegString.length)) {

      //滑动窗口为2
      if((n+1)<contentSegString.length){
        val line1=contentSegString(n)
        val line2=contentSegString(n+1)

        val temp1 = if (line1 != null) line1.split("/") else Array("")
        val word1_pos =if(temp1.length>1) temp1(1) else null

        val temp2 = if (line2 != null) line2.split("/") else Array("")
        val word2_pos =if(temp2.length>1) temp2(1) else null

        //如果前后两个关键词的词性都不为空
        if(word1_pos!=null && word2_pos!=null){
          //匹配模式一： n+n
          if(word1_pos.equals("n")&& word2_pos.equals("n")&& !cadiWordCopy.contains(temp1(0))&& !cadiWordCopy.contains(temp2(0))) {
            candiWord +=(temp1(0)->temp2(0))
            cadiWordCopy(0)=temp1(0)
            cadiWordCopy(1)=temp1(1)
          }
          //模式二： b+n
          if(word1_pos.equals("b")&& word2_pos.equals("n")&& !cadiWordCopy.contains(temp1(0))&& !cadiWordCopy.contains(temp2(0))){
            candiWord +=(temp1(0)->temp2(0))
            cadiWordCopy(0)=temp1(0)
            cadiWordCopy(1)=temp1(1)
          }
          //模式三： vn+n
          if(word1_pos.equals("vn")&& word2_pos.equals("n")&& !cadiWordCopy.contains(temp1(0))&& !cadiWordCopy.contains(temp2(0))) {
            candiWord +=(temp1(0)->temp2(0))
            cadiWordCopy(0)=temp1(0)
            cadiWordCopy(1)=temp1(1)
          }

          //模式四： nr
          if(word1_pos.equals("nr")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
          //模式五： nw
          if(word1_pos.equals("nw")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
          //模式六： ns
          if(word1_pos.equals("ns")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}

          //模式七： n，且词长度>2
          if(word1_pos.equals("n")&& temp1(0).length>2 && !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}

          //模式八： nz
          if(word1_pos.equals("nz")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}

        }else if(word1_pos!=null && word2_pos ==null){
          val line1=contentSegString(n)
          val temp1 = if (line1 != null) line1.split("/") else Array("")
          val word1_pos =if(temp1.length>1) temp1(1) else null

          if(word1_pos!=null ){
            if(word1_pos.equals("nr")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
            if(word1_pos.equals("nw")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
            if(word1_pos.equals("ns")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
            if(word1_pos.equals("nz")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
            if(word1_pos.equals("n")&& temp1(0).length>2 && !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
          }
        }
      }else {
        val line1=contentSegString(n)
        val temp1 = if (line1 != null) line1.split("/") else Array("")
        val word1_pos =if(temp1.length>1) temp1(1) else null

        if(word1_pos!=null ){
          if(word1_pos.equals("nr")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
          if(word1_pos.equals("nw")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
          if(word1_pos.equals("ns")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
          if(word1_pos.equals("nz")&& !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
          if(word1_pos.equals("n")&& temp1(0).length>2 && !cadiWordCopy.contains(temp1(0))) {candiWord +=(temp1(0)->"");cadiWordCopy(0)=temp1(0)}
        }
      }
    }

    //剔除重复项
    val candiWordSet=candiWord.toSet

    candiWordSet.toArray
  }
}