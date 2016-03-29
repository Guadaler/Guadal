import com.kunyandata.nlpsuit.sentiment.{Analyzer, Util}
import com.kunyandata.nlpsuit.util.WordSeg

import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2016/3/29.
  */
object Test_111 {
  def main(args: Array[String]) {
    var file="E:\\data_test\\data2\\neg\\煤价坠落 大型煤企探路套保.txt"
    var str="，，事件:央行发布公告,允许获准进入银行间市场的清算行和参加行开展债券回购交易,正回购的融资余额不得高于所持债券余额的100%,且回购资金可调出境外使用。，，，回购一小步,市场一大步。回购交易放开,利于扩大境外机构的债券投资和流动性管理需求。推动人民币国际化从跨境结算(货币的交易功能)转向境外持有(货币的储值功能)。:，|4，b\"，^*，l3，F9，l;，n，，，允许回购资金调出境外,将加大资金双向流动,提升在岸利率对离岸的基准影响力。目前境内外无论货币还是债券利率,短端的在岸价格均低于离岸,资金未必是流入,套利交易可能引发资金流出。#，c8，?)，Ih\"，H.，xU，，，离岸人民币市场快速扩张,贸易结算和套利套汇交易是推动境外人民币资金池快速增长的两大主因。庞大的境外人民币资金池,使得对人民币计价的资产和流动性需求与日俱增。3，z-，R,，#，J*，X*，P8，H$，U，，，境外机构在银行间交易规模和机构数量大幅增长。截止2014年末,共有211家包括境外机构获准进入银行间市场。截止2015年5月,有236家境外机构入市交易,持有债券5820亿,占比2%,处于不断增长趋势中。，，，金融市场开放,债市首当其冲,边际需求将增加。对境外投资者而言,债券市场是最获取人民币资产最便利、流动性最好的方式,对人民币债券的总量需求将不断提高。，，，下半年资本项目开放进一步加速,推动人民币实现跨境流通、投资和交易,资本市场开放增加、人民币换提高、跨境投融资更便利等一系列政策值得期待。"
    var str1="央行发布公告允许获准进入银行间市场的清算行和参加行开展债券回购交易"
    /*for(line <-Source.fromFile(file).getLines()){
      str +=line.trim()

      println()
      println(line)

      //用坤雁分词  逐句分词
      //    println(str)
      val result = WordSeg.splitWord(line,1)
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
    }*/

    //用ansj分词
//    var ansj_result=Analyzer.cut(str)
//    for(word <- ansj_result){
//      println(word)
//    }

    //用坤雁分词
    println("分词内容："+str)

    str=Util.replaceIllegal(str)

    val result = WordSeg.splitWord(str,1)
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
