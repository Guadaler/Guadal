package sentiment

import java.io.{PrintWriter, File}

import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.TextPreprocessing
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.{HashingTF, IDFModel, ChiSqSelectorModel}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by zx on 2016/4/27.
  */
object TestPredict {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Predict").setMaster("local")
    val sc = new SparkContext(conf)
    //    val testData="D:\\000_DATA\\predictTest\\【第五次标注】tagging"
    val testData = "D:\\000_DATA\\predictTest\\【第五次标注】tagging"
//        val testData="D:\\000_DATA\\predictTest\\【第七次标注】tagging（QQ_3300）"

//  val modelData = "D:\\000_DATA\\Model\\【第七次标注】\\2"
    val modelData="D:\\000_DATA\\Model\\【第五次标注】\\F_2_feature_2000"
//    val modelData="D:\\000_DATA\\Model\\8【第八次标注】\\Feature_1500"
//    val modelData="D:\\000_DATA\\Model\\8【第八次标注】\\Feature_2000"

    val models = PredictWithNb.init(modelData)

//    testPredict(sc,testData,models)

    //预测结果并写入本地
    val files = "D:\\000_DATA\\predictTest\\dzh_neg_3_title\\neg"
    val out = "D:\\000_DATA\\predictTest\\dzh_neg_3_title\\result(8)"
//    reLabelWithDZH2(sc,files,out,models)

    val files_neu ="D:\\000_DATA\\predictTest\\dzh_neg_3_title\\2 pre_neg_100"
    val out_neu = "D:\\000_DATA\\predictTest\\dzh_neg_3_title\\2 pre_result_100"
//    reLabelWithDZH2(sc,files_neu,out_neu,models)

    //用刘妙给的数据进行测试
    val file_liu="D:\\000_DATA\\predictTest\\new_liumiao\\new_liumiao.txt"
    val out_liu="D:\\000_DATA\\predictTest\\new_liumiao\\result"
    reLabelWithDZH3(sc,file_liu,out_liu,models)

  }

  /**
    * 用训练好对大智慧的数据进行机器标注，，作为新模型的训练集
    */
  def reLabelWithDZH3(sc: SparkContext, files: String, out: String, models: Map[String, Any]): Unit = {
    val stopWords = sc.textFile("D:\\111_DATA\\data\\stop_words_CN").collect()
    val stopBr = sc.broadcast(stopWords)
    var temp=""
    var count=0
    Source.fromFile(new File(files)).getLines().foreach(line=>{
      if(line.contains("===============================================") && count <=100){
        count +=1
        val result = PredictWithNb.predictWithSigle(temp, models, stopWords)
        result match {
          case "neg" => {
            val wr = new PrintWriter(out + "\\neg\\" + count+".txt", "utf-8")
            wr.write(temp)
            wr.close()
          }
          case "neu_pos" => {
            val wr = new PrintWriter(out + "\\neu\\" + count+".txt", "utf-8")
            wr.write(temp)
            wr.close()
          }
        }
        temp=""
      }else{
        temp +=line
      }
    })
  }

  /**
    * 用训练好对大智慧的数据进行机器标注，，作为新模型的训练集
    */
  def reLabelWithDZH2(sc: SparkContext, files: String, out: String, models: Map[String, Any]): Unit = {
    val stopWords = sc.textFile("D:\\111_DATA\\data\\stop_words_CN").collect()
    val fileList = new File(files).listFiles()
    fileList.foreach(file => {
      val content = Source.fromFile(file).getLines().map(line =>{line}).mkString
      val result = PredictWithNb.predictWithSigle(content, models, stopWords)
      result match {
        case "neg" => {
          val wr = new PrintWriter(out + "\\neg\\" + file.getName, "utf-8")
          wr.write(content)
          wr.close()
        }
        case "neu_pos" => {
          val wr = new PrintWriter(out + "\\neu\\" + file.getName, "utf-8")
          wr.write(content)
          wr.close()
        }
      }
    })
  }

  def testPredict(sc: SparkContext, testData: String, models: Map[String, Any]): Unit = {
    val stopWords = sc.textFile("D:\\111_DATA\\data\\stop_words_CN").collect()
    val stopBr = sc.broadcast(stopWords)

    val filemap = Util.readFile2HashMap(testData)
    var count_neg = 0
    var count_neg_r = 0
    var count_neu = 0
    var count_neu_r = 0

    var count = 0
    var count_r = 0

    val it = filemap.keySet().iterator()
    while (it.hasNext) {
      val file = it.next()
      val label = file.getParentFile.getName match {
        case "neg" => 1.0
        case "neu" => 4.0
      }
      val content = filemap.get(file).toString
      val result = PredictWithNb.predictWithSigle(content, models, stopWords)
      if (result == label) {
        count += 1
        count_r += 1
        result match {
          case "neg" => {
            count_neg += 1;
            count_neg_r += 1
          }
          case "neu_pos" => {
            count_neu += 1;
            count_neu_r += 1
          }
        }
      } else {
        count += 1
        result match {
          case "neg" => {
            count_neg += 1
          }
          case "neu_pos" => {
            count_neu += 1
          }
        }
      }
    }

    println("总数为：" + count + "   正确个数为：" + count_r)
    println("neg正确率：" + (count_neg_r.toDouble / count_neg.toDouble))
    println("neu正确率：" + (count_neu_r.toDouble / count_neu.toDouble))
    val right = count_r.toDouble / count.toDouble
    println("平均正确率为：" + right)
  }


  def testPredict2local(sc: SparkContext, testData: String, models: Map[String, Any]): Unit = {
    val stopWords = sc.textFile("D:\\111_DATA\\data\\stop_words_CN").collect()
    val stopBr = sc.broadcast(stopWords)

    val filemap = Util.readFile2HashMap(testData)
    var count_neg = 0
    var count_neg_r = 0
    var count_neu = 0
    var count_neu_r = 0

    var count = 0
    var count_r = 0
    val outPath = "D:\\000_DATA\\predictTest\\QQ_3300_error"

    val it = filemap.keySet().iterator()
    while (it.hasNext) {
      val file = it.next()
      val label = file.getParentFile.getName match {
        case "neg" => 1.0
        case "neu" => 4.0
      }
      val content = filemap.get(file).toString
      val result = PredictWithNb.predictWithSigle(content, models, stopWords)
      if (result == label) {
        count += 1
        count_r += 1
        result match {
          case "neg" => {
            count_neg += 1;
            count_neg_r += 1
          }
          case "neu_pos" => {
            count_neu += 1;
            count_neu_r += 1
          }
        }
      } else {
        count += 1
        result match {
          case "neg" => {
            count_neg += 1
            val wr = new PrintWriter(outPath + "\\neg\\" + file.getName + ".txt", "utf-8")
            wr.write(content)
            wr.close()
          }
          case "neu_pos" => {
            count_neu += 1
            val wr = new PrintWriter(outPath + "\\neu\\" + file.getName + ".txt", "utf-8")
            wr.write(content)
            wr.close()
          }
        }

      }
    }

    println("总数为：" + count + "   正确个数为：" + count_r)
    println("neg正确率：" + (count_neg_r.toDouble / count_neg.toDouble))
    println("neu正确率：" + (count_neu_r.toDouble / count_neu.toDouble))
    val right = count_r.toDouble / count.toDouble
    println("平均正确率为：" + right)
  }

  //预测
  def predictTest(): Unit ={
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("mltest").setMaster("local")
      val sc = new SparkContext(conf)

      val stopWords = sc.textFile("D:\\111_DATA\\data\\stop_words_CN").collect()

      //    val model=PredictWithNb.init("D:\\000_DATA\\Model\\8【第八次标注】\\8_Feature_2000_kunyan")
      val model=PredictWithNb.init("D:\\000_DATA\\Model\\【第五次标注】\\F_2_feature_2500")


      //        val content="菲律宾统计局近日发布“水稻和玉米生产预期报告”，称由于受连续干旱及台风影响，预计一季度水稻单产由3.93吨/公顷减少到3.85吨/公顷。 　讯：据《商业镜报》4月21日报道，菲律宾统计局近日发布“水稻和玉米生产预期报告”，称由于受连续干旱及台风影响，预计一季度水稻单产由3.93吨/公顷减少到3.85吨/公顷，产量达447万吨，比年初预测(459万吨)低2.6%，但同比增长3.7%。玉米单产由3.39吨/公顷下降到3.35吨/公顷，产量达241万吨，比年初预测(244万吨)低2.6%，同比增长5.7%。"
      //    val content="　　昨天我们指出上证维持震荡整理行情，创业板再冲高后也有整理要求。周三上证继续无聊的横盘震荡整理，而创业板则收出一根阴线。依靠创业板中的指标股的上涨创业板仍维持小幅整理，但前期不少龙头股依然下跌，老的龙头热点无力回升，说明短期震荡将加大。涨幅榜上，由于油价再次下跌，民航板块上午大涨，收盘时位列涨幅榜首位。福建自贸、粤港自贸等涨幅次之，手游概念股在几只龙头股的带领下涨幅也居前。\n　　后市方面，上证维持震荡整理行情，创业板冲高回落收阴线短期震荡也将加大。激进的投资者可在互联网、电子信息等板块中寻找补涨股。"
      //    val content="　　电视美食真人秀节目热播，一道姜蓉鸡拍出180万元“天价”\n　　“厨艺竞拍”能让餐饮业掀起“创新热”吗？\n　　□本报记者祝瑶\n　　跟着真人秀节目觅寻商机，这对眼疾手快的商家来说已不是新鲜事了。上周五开播的《中国味道》玩起了“选手秀菜+BOSS竞拍”的节目模式，现场不仅是“口水直流”的厨艺秀场，更因为选手现制的一道传承菜姜蓉鸡，引来现场大佬们争相竞拍。最后由3位餐饮大佬联合拍出了180万元的“天价”。\n　　餐饮界业内人士指出，当荧屏上进行着“味蕾大战”的同时，“厨艺竞拍”的真人秀模式，能否为现在的餐饮“创新热”再添一把火，需要时间和市场的检验。\n　　一道姜蓉鸡拍出180万元，美食真人秀“身价”创新高\n　　在上周五晚亮相的《中国味道》第一期节目中，不少人一眼就认出了杭城餐饮大佬外婆家创始人吴国平、小南国董事长王慧敏等国内餐饮领军人的身影。\n　　变身为“餐饮好声音”，作为唯一的一位杭州籍“导师”，外婆家创始人吴国平，一边观赏节目一边在朋友圈发截图自嘲，“下期我争取熟练点”。据外婆家相关人士透露，节目是在8月中旬进行首次录制的，第一天拍摄就经历了19个小时，直到凌晨4点半才收工。\n　　在首期节目中，原创的“选手秀菜+BOSS竞拍”的新玩法，让来自广东梅州的选手郭科成为《中国味道》开播以来的最大赢家。\n　　自幼就是乡厨家宴的掌勺人，一上场，郭科就以八行口诀，让10位餐饮领军人组成的BOSS团，对从祖父这一代相传而来的姜蓉鸡充满了兴趣。\n　　“生姜竖拍成丝，挤汁加盐腌鸡；腌制一刻风干，风干一刻即蒸；原只清蒸一刻，斩件摆回原只；姜丝加盐炸蓉，姜蓉铺鸡上桌。”在烹饪现场，这道传家菜姜蓉鸡，通过郭科的巧手和奇思，生姜通过竖拍成丝，挤汁加盐，并将汁丝分离后的姜蓉入油锅。待到出油锅时，姜蓉已然神似肉松。\n　　值得一提的是，腌鸡过程中，郭科不仅“好脾气”地给鸡“按摩”，还拿出了小电扇吹鸡，为的就是在鸡肉氧化过程中尽快排酸。好不容易等到姜蓉铺鸡上桌，在场的各位嘉宾已是“口水直流”。\n　　除了菜品口感引来大佬纷纷点赞外，这道菜居然得到了小南国董事长王慧敏、眉州东坡集团董事长王刚和筷道餐饮集团董事长谷峰高达180万元的联合竞价，只为共同赢得这道传家菜姜蓉鸡的技术转让。\n　　荧屏“味蕾大战”正酣，掘金美食真人秀花招各出\n　　随着美食纪录片《舌尖上的中国》第一季、第二季相继收视大捷，国内的美食市场也因此迎来了消费热潮。\n　　嗅觉灵敏的食品电商抢在餐饮行业前开动，打起“舌尖美食”商战。一时间，口感鲜美却少有人知的蕨根粉、雁来蕈等低调的“山货”变成了人尽皆知，香格里拉松茸、毛竹林冬笋、林芝蜂蜜也时不时出现在杭城吃货的餐桌上。\n　　不仅是最火的《舌尖》系列，为了争抢美食市场，去年以来，顺势而出的美食真人秀节目《顶级厨师》、《厨王争霸》、《十二道锋味》，同样也使得节目同款美食热卖的商机频频涌现。在节目中，玩的不仅仅是真功夫绝活“斗菜”，往往还加入明星情节、民间故事、情感传承等新鲜元素。\n　　“与以往单纯展示烹饪过程的美食服务类节目相比，这次餐饮大佬们直接竞拍的美食真人秀模式，也算是给美食节目创新提供了新的思路。”有业内人士指出，与“以点论价”的硬广投放相比，美食类节目拥有更广阔的广告植入空间，比如《中国味道》中“避无可避”的厨具、餐具，食材的配备，以及投资性合约和创业基金的提供等。\n　　与单纯的线上延伸到线下的O2O相比，也有业内人士把这场引入了“BOSS竞拍”环节的《中国味道》的与同样以“T台秀+买家竞拍”的《女神的新衣》(后更名为《女神新装》)的时尚真人秀相提并论。\n　　首播中，杭城吃货小王发现，美食真人秀节目甚至还引入了时下流行的“摇电视”技术。“打开微信中的摇一摇界面，晃动手机便可轻松进入节目互动页面，还有机会摇到不少合作餐厅美食优惠券和厨房电器代金券。”\n　　对于掘金美食真人秀节目，业内人士指出，广告之外的其他衍生产品收益也指日可待，比如“MasterChef”就不仅有电视的版权，还有衍生品开发的版权，后续会出版菜谱、引入代言等。为此，当年引入该档节目中国版的东方卫视，也成立了专门的部门做衍生产品，为未来的产业经营做储备。\n　　“厨艺竞拍”模式，能否为餐饮“创新热”再添一把火？\n　　对于餐饮界拿出真金白银在电视节目中“抢拍”新菜品，杭城餐饮界业内人士认为，创下新高的180万元，不但是美食的价值，更是传承的价值。也有人指出，“厨艺竞拍”的真人秀模式，能否为餐饮“创新热”再添一把火，还很难说。\n　　有餐饮圈人士直言，“《女神的新衣》玩的是T2O模式，是一种‘即播即买’，‘价值即时转换’的模式，但需要现场烹饪的一道菜，却没法这样玩下去。”\n　　更重要的是，从可操作性来看，即便餐饮大佬重金买断了技术转让，但一道菜从引入厨房到烹饪上桌、从单一门店到迈向全国连锁，依然有长路要走。“因为地域的原因，一道传承菜要想征服外地消费者的胃，也需要一定程度的‘改良’。”有餐饮人士表示担心，180万的“噱头”短期内可带动人气，但未必能达到长效。此外，中餐的标准化难题也不容忽视。\n　　在“秘密不多”的餐饮圈里，曾经就有餐饮大佬自曝，餐饮业创新真是件难事，团队刚琢磨出一个新点子，背后至少得有三四个备用点子，要不然这边新菜刚上马，那边很可能就被“复制”得满大街可见。\n　　记者了解到，杭城发展较为成熟的餐饮品牌对菜品研发也是不遗余力，基本上都设立了“餐饮技术部”，招至麾下的基本上都是餐饮总监、厨师长，外出寻味、内部交流、斗菜斗味以及新品试菜都是每月必不可少的事儿，一年下来新菜研发的投入经费也是很可观的。\n　　在杭州西湖银泰城的外婆家，就上新了老底子杭州路边有名的小吃“油墩儿”、“定胜糕”，但想要迎合年轻吃货的喜好，研发团队在前期还是经过了数次改良；当有着浙江地方特色的越式醉鸡、温州鱼滑、东海三拼，摆在杭城消费者面前时，也是花了功夫的“升级版”。\n　　最近风头正甚的“蒸年青”，同样历经了数次内测、公测，主事人金宏伟对菜品的改良也是一轮接一轮。例如，“蒸年青”粉丝喜爱的一道鲜虾蒸臭豆腐，经过口味甄选，才选定了用带有浓厚芝麻香的桐庐臭豆腐。\n　　与此同时，杭城一批新崛起的“90后餐厅”的老板们更是充满了危机意识，每一两个月就要飞往亚洲餐饮潮流的前沿阵地，带回第一手线报，对菜品再次进行改良创新后投放餐厅。"
      val content="据中央纪委监察部网站消息,经贵州省委批准,贵州省纪委对七冶建设有限公司党委委员、总工程师张泽进涉嫌严重违纪问题立案审查,其涉嫌违法问题直接移送司法机关调查处理。张泽进简历:张泽进,男,土家族,1970年出生,贵州思南人,中共党员,大学本科学历,1991年7月毕业于北方工业大学工业与民用建筑专业,同年在中国有色第七冶金建设公司参加工作,现任七冶建设有限责任公司党委委员、总工程师,七冶路桥工程有限责任公司董事长,七冶金沙建设项目有限公司法人代表。"

      val result=PredictWithNb.predictWithSigle(content,model,stopWords)
      println(result)

      //测试概率方法
      println("【测试输出概率的方法】")
      //对文本[分词+去停]处理
      val wordSegNoStop = TextPreprocessing.process(content, stopWords)

      //用模型对处理后的文本进行预测
      val prediction = model("nbModel").asInstanceOf[NaiveBayesModel]
        .predictProbabilities(model("chiSqSelectorModel").asInstanceOf[ChiSqSelectorModel]
          .transform(model("idfModel").asInstanceOf[IDFModel]
            .transform(model("tfModel").asInstanceOf[HashingTF]
              .transform(wordSegNoStop))))

      println(prediction)

      //
      println("[调用函数]")
      val result2=PredictWithNb.predictProbabilities(content,model,stopWords)
      result2.foreach(line =>println(line._1+": "+Math.round(line._2*10000)/10000.0000+"%"))
      result2.foreach(println(_))


      println(scala.math.round(0.0001))
      println(scala.math.round(0.9999))
      println(scala.math.round(99.99))
      println(scala.math.round(63.25))
      println(scala.math.round(0.6352))

      val a=15.32743859;
      val b=Math.round(a*10000)/10000.0000;//保留四位小数
      println(b);
    }
  }
}
