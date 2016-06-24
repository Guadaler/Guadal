package wordExtraction

import com.kunyandata.nlpsuit.util.AnsjAnalyzer

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/3/18.
  * 文本标准化处理过程
  */
object TextPreprocessing {

  /**
    * 格式化文本，转化空白字符为停用词表中的标点符号，同时统一英文字母为小写
    *
    * @param content 原始文本字符串
    * @return 返回格式化之后的字符串
    */
  def formatText(content: String): String = {

    val step = 65248
    val sbcStart = 65281.toChar
    val sbcEnd = 65374.toChar
    val sbcSpace = 12288.toChar
    val dbcSpace = 32.toChar
    val bufferString = new ArrayBuffer[Char]

    if(content == null) {
      content
    } else {
      content
        .replaceAll("<[^<]*>", "")
        .replaceAll("&nbsp", "")
        .foreach(ch => {
        if (ch == sbcSpace){
          bufferString.append(dbcSpace)
        }else if (ch >= sbcStart && ch <= sbcEnd){
          bufferString.append((ch - step).toChar)
        }else{
          bufferString.append(ch)
        }
      })
      bufferString
        .mkString
        .replaceAll("""\s""", "")
        .replaceAll("\"", ",")
    }
  }

  /**
    * 去除分词结果中的标点符号和停用词
 *
    * @param content 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWords(content: Array[String], stopWords:Array[String]): Array[String] = {
    if (content !=null) {
      var result = content.toBuffer
      stopWords.foreach(stopWord => {
        if (result.contains(stopWord)){
          result = result.filterNot(_ == stopWord)
        }
      })
      result.toArray
    } else {
      null
    }
  }


  /**
    * 实现字符串的分词和去停,并分装成方法 ，与上面的process()流程相同，只是分词采用ansj
    * @param content 需要处理的字符串
    * @param stopWords  停用词
    * @return 返回分词去停后的结果
    * @author zhangxin
    */
  def process(content: String, stopWords:Array[String]): Array[String] = {

    // 格式化文本
    val formatedContent = TextPreprocessing.formatText(content)

    // 实现分词，并根据词性过滤
    val resultWords = AnsjAnalyzer.Nlp_cut(formatedContent)
    val extraction=resultWords.map(line =>{
      val word2=line.toString
      val word=if(word2.contains("/n")||word2.contains("/ns")||word2.contains("/nt")||word2.contains("/nr")||word2.contains("/nw")){
        val temp2=word2.split("/")
        temp2(0)
      }else{
        "###"
      }
      word
    }).filterNot(_.contains("###"))

    // 实现去停用词
    if (extraction == null)
      null
    else
      removeStopWords(extraction.toArray, stopWords)
  }

  def main(args: Array[String]) {
    val content="　　下游企业减产，LNG需求下降，京津冀地区运贸商大部分暂停了贸易运输计划。\n　　据安迅思采访的河北地区多家物流商称，配合阅兵期间环保要求，河北省煤场、钢厂等三千多家企业限停产，运输重卡纷纷停运，当地加气站生意寥寥，需求量大减。部分物流商表示，物流车辆将暂时停止在京津冀地区活动，或将车调去山西、华东地区进行配送等。\n　　此外，高速严查限行导致道路运输不畅。据河北省公安厅交管局26日通报，为做好抗战胜利70周年纪念活动期间空气质量保障工作，该省机动车将从8月28日0时至9月4日24时实施限行。尤其是石家庄、唐山、廊坊、保定、衡水、邢台、邯郸市，定州、辛集、迁安、涿州市等城市，将作为重点监控对象。\n　　通告显示，将停驶运输土方、渣土等扬尘车辆、危化品运输车。利用环京路网，对进省过境北京的低于国Ⅲ标准的车辆实施远端分流绕行。通过7条高速公路进京的，组织车辆绕行张涿、廊涿、荣乌、京台、长深高速（600548）公路及112国道。通过10条国省道进京的，在与112国道交汇处设置分流点组织车辆绕行112国道。此次道路限行影响较大的城市为石家庄、廊坊、保定、沧州等临近北京的城市，道路管控较严，限制路段较多。\n　　多位贸易商表示，如果遇到长期客户需要保障用气，只能改走国道，但是国道堵车使运输周期变长、运距增加约50-60公里，运贸商们表示为维护长期用户且仅时间较短，多自行承担。西北地区通往山东方向的车辆，多走青银高速或国道，此路段不受限制，运贸商多无压力。\n　　京津冀地区LNG工厂恐遭检修。此次为保障阅兵式限行，执行力度空前严格。市安监局、消防等部门至液化工厂，要求进行检修，“省级安全领导小组都会下来巡视，对于生产跟销售方面，虽在努力协商中，但检修可能性依旧达90%”，河北某液厂人士透露道"

    // 格式化文本
    val formatedContent = TextPreprocessing.formatText(content)

    // 实现分词
    val resultWords = AnsjAnalyzer.Nlp_cut(formatedContent)

    resultWords.map(line =>println(line))

    //根据词性过滤
    val extraction=resultWords.map(line =>{
      println(line)
      val word2=line.toString
      val word=if(word2.contains("/n")||word2.contains("/ns")||word2.contains("/nt")||word2.contains("/nr")||word2.contains("/nw")){
        word2
      }else{
        "###"
      }
      word
    }).filterNot(_.contains("###"))
  //  val extractionFilter=extraction.filter()


    println("---------------------------------------------------------------------")
    extraction.foreach(println(_))

    println(resultWords.size+"   ====   "+extraction.size)

  }
}