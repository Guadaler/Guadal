package wordExtraction

import java.text.SimpleDateFormat
import java.util.Date


/**
  * Created by zhangxin on 2016/6/13.
  */
object Test {
  def main(args: Array[String]) {
//    var line ="周四，国内豆类品种从主力合约看，豆一1509合约收于4393元/吨，较前一日结算价下跌0.05%；豆粕1509合约收于2556元/吨，较前一日结算价上涨0.79%；豆油1509合约收于5754元/吨，较前一日结算价下跌0.31%。\n　　现货方面，河北唐山地区油厂豆粕价格：43%蛋白：2620元/吨；广西防城港地区油厂豆粕价格：43%蛋白：2530元/吨；江苏盐城地区油厂豆粕价格：43%蛋白：3040元/吨。辽宁大连：工厂报价，一级豆油5970元/吨，三级豆油5900元/吨；天津地区：中纺报价，一级豆油5950元/吨；浙江宁波：工厂报价，一级豆油5950元/吨。\n　　据美国地球卫星气象(EarthSatWeather)旗下的农业气象机构MDACropCast发布的预测数据显示，2015/16年度全球大豆产量预计为3.027亿吨，和上周发布的数据持平。相比之下，美国农业部预计2015/16年度全球大豆产量为3.173亿吨，略高于2014/15年度的3.172亿吨。MDACropCast维持2015/16年度美国大豆产量预测值不变，仍为38.23亿蒲式耳，比2014/15年度创纪录的产量减少3.6%，相比之下，美国农业部的预测为38.5亿蒲式耳。MDACropCast公司本周继续预测2015/16年度巴西大豆产量为9830万吨，阿根廷大豆产量预计为5700万吨，均和早先预测值持平。\n　　操作上，豆一1509合约冲高回落，期价连续四天收阴，短期观望为宜；豆粕1509合约超跌反弹，前期空单可以分批止盈，剩余空单依托10天均线持有；豆油1509合约上试20天线未果，短期20天均线存在一定的压制作用，趋势投资者暂时观望为宜。"
//    line=line.replaceAll("[^(a-zA-Z\\u4e00-\\u9fa5)]", "")
//    println(line)

//    val a=3
//    val b=1.5
//    val result = -a * math.log(b)
//    println(result)
//    println(math.log(10))
//    println(math.log(0.8))
//    println(math.log(12))
//    println(math.log(0.65))
//
//    val c=Array("wo","shi","zhong","guo")
//    println(c.indexOf("zhong"))

//    val content="打卡小红椒,阿姆斯特丹鹿特丹银行\n道琼斯股票价格平均数\n东方汇理与苏伊士银行\n东海证券有限责任公司\n华泰证券有限责任公司\n平安证券有限责任公司\n中国国际信托投资公司\n中信证券股份有限公司\n捷克和斯洛伐克国家银行"
//    val conf = new SparkConf().setAppName("wordExtraction").setMaster("local")
//    val sc = new SparkContext(conf)
//    val user=Array("C:\\Users\\Administrator.HTBJUOLFUL4H7CK\\Desktop\\test.txt")
//    AnsjAnalyzer.init(sc,user)
//    val contentSeg=AnsjAnalyzer.Nlp_cut(content)
//    contentSeg.foreach(word=>println(word.toString))

    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var hehe = dateFormat.format( now )
    println(hehe)
  }
}
