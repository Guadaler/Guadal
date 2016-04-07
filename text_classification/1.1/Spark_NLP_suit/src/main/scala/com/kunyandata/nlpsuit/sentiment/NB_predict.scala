package com.kunyandata.nlpsuit.sentiment

import java.io.{PrintWriter, File, FileInputStream, ObjectInputStream}

import com.kunyandata.nlpsuit.util.TextProcess
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{ChiSqSelectorModel, HashingTF, IDFModel}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by zx on 2016/3/28.
  */
object NB_predict extends App{

  val conf =new SparkConf().setAppName("test").setMaster("local")
  val sc=new SparkContext(conf)

  val modelMap_f=init("D:\\000_DATA\\Model\\【第三次标注】\\F")
  val modelMap_s=init("D:\\000_DATA\\Model\\【第三次标注】\\S")
  val model=Array[Map[String, Any]](modelMap_f,modelMap_s)

  val stopWords=Source.fromFile("D:\\111_DATA\\data\\stop_words_CN").getLines().toArray  //读取停用词典并转成Array
  val stopWordsBr = sc.broadcast(stopWords)

  //单模型+单篇文章
  val content1 = "本期高频数据显示：1月信贷扩张剧烈达历年之最，同时受人民币外汇市场波动与疲弱出口影响，宽口径外汇占款下降显著，2月上旬央行货币净投放更创历史新高。\n　　信贷极度宽松背景下利率市场表现稳定，只有信托类产品收益率在2月上旬显著下降。信贷扩张还未传导至实体经济，其中工业方面钢铁行业去产能进一步加速并为价格托底，煤炭、水泥价格依旧徘徊谷底；航运价格仍持续走低；受春节假期影响新房、二手房与土地交易量均表现惨淡。\n　　2月5日当周央行公开市场货币净投放为3300亿元，尽管比上期6900亿有所下降，但仍保持历史高位。2月上旬银行间市场隔夜回购利率上升29个基点至2.26%，7天回购利率下降4个基点至3.06%。2月上旬国债收益率基本维持不变，AAA级与AA级企业债收益率分别上升2个基点。AA+级收益率小幅下降。\n　　钢铁的消费中，基建和房地产超过50%，机械和汽车接近25%，所以钢铁的供求和价格数据是观测固定资产投资和消费的重要指标。钢铁去产能步伐加速，价格回落。1月下旬粗钢产量同比增长-10.81%，连续5旬出现两位数负增长。2月上旬唐山钢坯价格维持在1540元/吨，为近一个月来新低。1月出口（除中国香港地区外）同比增长-12.33%，是近10个月来最大负增长。\n　　1月份新增社会融资总额34173亿元，环比猛增88%，增长主要来源于新增人民币贷款项（同比增长72%，达到25370亿元）。\n　　1月金融机构新增外汇占款减少23766亿元，达到11年来的最大当月减少值。1月中国百城新建住宅价格上升0.42%，其中一、二、三线城市环比分别上升1.89%、0.80%、-1.08%。2014年房地产投资占GDP14.9%，占固定资产投资的18.6%，房地产贷款余额占总贷款余额比21.27%，土地出让金占政府收入30.4%。"

  //pos
  /*val content2="　　上网电价和销售电价近日首次同步下调，煤炭市场分析专家李廷认为，当前煤价已处低位，电价下调或给煤市带来积极影响。\n　　李廷表示，国家此次下调燃煤发电上网电价，是2013年以来调整幅度最大的一次。对燃煤发电企业来说，每千瓦时电价下调2分钱，其效果等同于发热量5500大卡电煤价格每吨上涨50元。为维护企业盈利水平，电价下调后发电企业可能会提出进一步降低煤价的要求，甚至部分电企将通过暂时减少采购等手段迫使煤企降价。\n　　然而，李廷认为，当前环渤海5500大卡动力煤平仓价已降至420元／吨，与2012年12月《关于深化电煤市场化改革的指导意见》提出煤电联动机制时比，吨煤降价近200元。目前，绝大多数煤企已经亏损或频临亏损，电价下调后，煤价进一步回落空间有限。\n　　李廷认为，此次电价下调，或给煤市带来积极影响。首先，煤企销售难度必然加大，或将促使部分煤矿尽早关停，有助于煤炭行业深度调整和煤炭市场重新趋于平衡。其次，煤电价格联动有助于稳定发电行业市场预期，构建合作共赢的良性煤电关系。再次，有助于电力需求回升，最终带动电煤需求好转。\n　　此外，销售电价降低，一定程度上也会降低煤炭生产成本。对于山西、内蒙古和陕西的煤炭企业来说，如果电价每千瓦时下降1.8分，吨煤生产成本或降低0.2至0.3元。"
  val content3="　　新华网北京7月31日电(记者郭兴华)据新华社全国农副产品和农资价格行情系统监测，与前一日相比(下同)，7月31日，猪肉、禽蛋价格上涨；蔬菜价格以涨为主；牛羊肉、水产品、水果、食用油价格略有波动；成品粮、奶类价格基本稳定。\n　　猪后臀尖肉、猪五花肉价格分别上涨0.1%、0.2%；牛腩肉、去骨鲜羊肉价格下降0.1%，牛腱肉价格涨幅不足0.1%，带骨鲜羊肉价格上涨0.1%；监测的21种蔬菜中，13种价格上涨，3种价格下降，5种价格持平。\n　　监测数据显示，4月中旬以来全国猪肉价格呈上涨走势。与前一日相比，31日，近五成省区市猪五花肉价格上涨，其中内蒙古、上海、云南的价格涨幅最大，价格分别上涨1.9%、1.8%、1.7%，其余省区市价格涨幅均小于1.0%。"
  val content="　　本报记者杨志锦北京报道 \n　　猪肉价格正开启新一轮上涨。\n　　数据显示，猪肉价格已从4月初的19.63元/千克上涨至目前的25.98元/千克，涨幅近三分之一。\n　　生猪供给收缩，生猪和能繁母猪存栏量均降到最低点，被市场人士认为是本轮猪价上涨的主要原因。即使养殖户们现在补栏，有效供给也要到年底才能形成。肉价上涨趋势至少还将持续一年时间。\n　　CPI随之被牵动。国家统计局最新公布数据显示，6月猪肉价格上涨7.0%，影响居民消费价格总水平上涨约0.20个百分点。\n　　“猪价上涨带动CPI出现高点的时间很可能在明年上半年。”华泰证券（601688）首席宏观研究员俞平康在他发布的研究报告中判断，猪肉价格对CPI的拉动在2016年1-3月份达到1.5个百分点左右，形成一个CPI的阶段性高点。\n　　货币政策也因此面临一定的变数。\n　　“猪肉价格如果急速上涨，它对货币政策会有影响，但是货币政策不会因此而迅速转向。”俞平康7月23日向21世纪经济报道记者表示。他发布的上述研究报告称，今年下半年货币政策仍然可以保持一个宽松的政策取向，但是明年年初货币政策的宽松空间会因为CPI的同比回升而受到掣肘。\n　　“这才刚刚开始”\n　　数据显示，今年初，猪肉价格在20元/千克价位附近小幅波动。从4月初开始，持续上涨趋势出现，迄今已达到25.98元/千克，涨幅32%。\n　　“这一轮猪肉价格上涨的主要原因是供给收缩遭遇需求企稳。”民生证券宏观研究员朱振鑫对21世纪经济报道记者表示。\n　　从供给端来看，2012年以来生猪养殖业长期亏损，养殖户大批退出，生猪存栏量持续下降。以生猪存栏(能繁母猪)为例，2015年6月生猪存栏(能繁母猪)的数量已降至3899万头，为历史最低点，比之前的最低纪录还低16.8%。从需求端来看，中央“八项规定”对餐饮消费的压缩效果正在递减，加上宏观经济有企稳迹象，餐厅、家庭猪肉消费重新提振。\n　　值得注意的是，本轮猪价上涨行情并未带动生猪补栏。今年4月份以来生猪存栏量仍在下滑。“主要因为能繁母猪的剧烈淘汰导致仔猪供给收缩，限制了生猪补栏。”朱振鑫说。\n　　他解释称，仔猪供给收缩也反映在其价格上，年初至今仔猪价格上涨幅度达123%。根据生物周期，仔猪补栏到出栏需5个月时间，即使现在补栏，也需要到年底才能产生有效供给。“通常下半年猪肉需求较上半年相对旺盛，年底前生猪供给压力将持续紧张，猪价上涨才刚刚开始。”\n　　一位不愿具名的证券公司农业行业分析师也认为，2012年以来超长时间的亏损对行业参与者的资产负债表破坏较为严重，生猪产能调整的幅度很大，未来生猪养殖行业上行的周期将拉长。“肉价至少要涨一年，现在25元，一年内将上涨20%，达到30多元，超过2011年的高点没有问题。”\n　　在2011年的猪肉上涨行情中，上涨周期从2010年6月至2011年9月，持续15个月；肉价达到30.41元/千克的高点，涨幅105%。\n　　齐鲁证券农业组的一份研究报告则称，2015年中国生猪小幅供不应求，2016年供需差额为29.7百万头，出现明显供不应求(详见本版数据图)。预计猪价上涨行情将延续1年余至2016年9-10月，高点比肩2011年。\n　　在供给收缩、需求企稳的大背景下，俞平康则认为，和2011年那轮猪价上涨相比，养猪的劳动力成本和饲料成本均有较大提高，而两者的提高将使猪肉价格反弹至更高高度。\n　　明年上半年形成CPI高点\n　　猪价牵动CPI走向。虽然国家统计局没有公布CPI组成商品的所占权重，但大多数经济学家估计食品所占权重约为30%。而猪肉在食品部分所占比重约为三分之一，故猪肉在整个一篮子商品中所占比重约为10%，为所占权重最大的单一组成商品。\n　　因此，从历史数据来看，猪肉价格和CPI变动趋势一致，呈正相关关系。如2011年7月，猪肉价格上涨56.10%，当月CPI涨幅达6.45%。猪肉价格上涨对CPI的拉动达到1.37个百分点。\n　　国家统计局最新发布数据显示，今年6月猪肉价格上涨7.0%，影响居民消费价格总水平上涨约0.20个百分点。随着猪肉价格节节攀升，这一拉动效果将不断显现。\n　　朱振鑫根据历史数据计算，猪肉价格同比与CPI新增涨价因素相关系数约为0.04。他预计今年猪肉价格较2014年的均价上涨28.6%，因此今年猪肉价格上涨贡献CPI新涨价因素1.14%。\n　　“因为本轮猪价上涨将至少持续一年时间，同时由于今年上半年CPI基数低，猪价上涨带动CPI出现高点很可能在明年上半年出现。”前述农业行业证券分析师表示，“CPI是一个同比数据，今年上半年基数低，因此明年上半年压力最大。”\n　　齐鲁证券农业组的测算结算结果显示，2015、2016年两个年度猪价上涨对CPI的贡献将分别达到0.51与0.67个百分点。从月度数据来看，目前猪价同比一路上升，至2016年2月猪肉价格同比增速达到峰值，同期通胀可能上升至3%。\n　　俞平康则认为，2015年1-3月份猪肉价格出现下跌，这意味着这轮猪周期最大的同比涨幅将出现在2016年1-3月份，彼时也是对CPI拉动最大的月份。如果2016年上半年猪肉价格上涨到30元/千克以上，可能最高将拉动CPI上升1.5个百分点以上，形成阶段性高点。这时猪肉价格对CPI的拉动将超过2011年的峰值。\n　　货币政策或转向\n　　今年以来CPI在2%以内低位运行，货币政策得以保持相对宽松态势。但俞平康经测算指出，随着猪肉价格进一步攀升，今年7-12月份CPI同比会逐渐走高，至年底CPI有可能达到3.0%左右。这或将影响货币政策的走向，因为3%是今年政府工作报告所宣布的CPI涨幅的控制上限。\n　　基于上述分析，俞平康认为今年7-12月份货币政策仍然可以保持一个宽松的政策取向，但是明年年初，货币政策的宽松空间会因为CPI同比回升而掣肘。届时可能迎来货币政策的转向。\n　　朱振鑫的判断有所不同。他认为，尽管猪价上涨给CPI带来一定压力，但考虑到经济总需求仍然不强，CPI年内触碰政策上限的可能性不大。“货币政策会因为猪价上涨而受到一定掣肘，但因此转向的概率较低。”\n　　前述不愿具名的证券公司农业行业分析师认为，这一轮猪肉涨价的宏观背景与上一轮不同。2011年那一轮肉价上涨，缘于中央政府为应对2008年金融危机而在2009年、2010年扩大了货币供应量，年度通货膨胀率达到5.4%，使猪肉供应量相对变少，肉价因此上涨。其时经济增速高达9.2%。\n　　“上一轮猪价上涨的宏观环境是高通胀高增长，而这一次则有可能出现高通胀而经济增速不高的局面。此轮猪价上涨推升CPI的同时，宏观经济尚未企稳，仍有较大下行压力。”这位不愿具名分析师说，在这种情况下，实施宏观调控的难度必会加大。(编辑谭翊飞)"

  val result =predict(content, modelMap_f, stopWordsBr)
  println("预测结果为： "+result)*/

  //二级模型+批量文章
  val filePredictPath="D:\\000_DATA\\predictTest\\predictData"
  val outPredictPath="D:\\000_DATA\\predictTest\\predictResult.txt"
  predictMany(filePredictPath,outPredictPath,model,stopWordsBr)




  /**
    * 初始化读取模型
    *
    * @param path  模型路径
    * @return 模型Map[模型名称，模型]
    */
  def init(path: String): Map[String, Any] = {
    val fileList = new File(path)
    val modelList = fileList.listFiles()
    var modelMap:Map[String, Any] = Map()
    modelList.foreach(cate => {
      val modelName=cate.getName
      val tempModelInput = new ObjectInputStream(new FileInputStream(cate))
      modelMap += (modelName -> tempModelInput.readObject())
    })
    modelMap
  }

  /**
    * 情感预测
    * @param content  待预测文章
    * @param models  模型Map[模型名称，模型]，由init初始化得到
    * @param stopWordsBr 停用词
    * @return  返回情感label
    */
  def predict(content: String, models: Map[String, Any], stopWordsBr: Broadcast[Array[String]]): Double = {
    val wordSegNoStop = Util.process_ansj(content, stopWordsBr)
    val prediction = models("nbModel").asInstanceOf[NaiveBayesModel]
      .predict(models("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
        .transform(models("idfModel").asInstanceOf[IDFModel]
          .transform(models("tfModel").asInstanceOf[HashingTF]
            .transform(wordSegNoStop))))
    prediction
  }

  /**
    * 二级模型 +单篇文章
    * @param content  文章内容
    * @param arr  二级模型数组
    * @param stopWordsBr  停用词表
    * @return 预测label
    */
  def predictOne(content:String,arr:Array[Map[String, Any]],stopWordsBr: Broadcast[Array[String]]): String ={
    var temp = predict(content,arr(0), stopWordsBr)
    if (temp == 4.0) {
      temp = predict(content,arr(1), stopWordsBr)
    }
    val result=replaceLabel(temp)
    result
  }

  /**
    * 二级模型，批量预测
    * @param filepath  批量预测文章路径
    * @param outpath  输出预测结果
    * @param arr  二级模型数组
    * @param stopWordsBr  停用词表
    */
  def predictMany(filepath:String,outpath:String,arr:Array[Map[String, Any]],stopWordsBr: Broadcast[Array[String]]): Unit ={
    val wr=new PrintWriter(outpath,"utf-8")
    val files=new File(filepath).listFiles()
    for(file <-files) {
      val title = file.getName.substring(0, file.getName.indexOf(".txt"))
      var contentstr = ""
      for (line <- Source.fromFile(file).getLines()) {
        contentstr += line
      }
      var temp = predict(contentstr,arr(0), stopWordsBr)
      if (temp == 4.0) {
        temp = predict(contentstr,arr(1), stopWordsBr)
      }
      val result=replaceLabel(temp)
      wr.write("【" + result + "】" + title + "\n")
      wr.flush()
    }
  }

  /**
    * 编号替换成标签  如 1.0 =》 neg
    * @param tempresult
    * @return
    */
  def replaceLabel(tempresult:Double): String ={
    val result=
      tempresult match {
        case 1.0 => "neg"
        case 2.0 => "neu"
        case 3.0 => "pos"
      }
    result
  }
}
