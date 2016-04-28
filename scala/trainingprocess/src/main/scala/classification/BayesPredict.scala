package classification

import com.kunyandata.nlpsuit.util.{TextPreprocessing, WordSeg}
import com.kunyandata.nlpsuit.classification.Bayes
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by QQ on 2016/4/19.
  */
object BayesPredict {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BayesPredictTest")
      .setMaster("local")
//      .set("spark.local.ip", "192.168.2.65")
      .set("spark.driver.host", "192.168.2.65")
//      .setMaster("spark://222.73.57.12:7077")
//      .set("spark.local.ip","222.73.57.12")
//      .set("spark.driver.host","222.73.57.12")
//      .set("spark.executor.memory", "15G")
//      .set("spark.executor.cores", "4")
//      .set("spark.cores.max", "8")
    val sc = new SparkContext(conf)
    //    val modelMap = initModel("/home/mlearning/Models/", hdfs = false)
    //    val stopWords = getStopWords("/home/mlearning/dicts/stop_words_CN", hdfs = false)
    val modelMap = Bayes.initModels("/home/QQ/mlearning/Models")
    val modelMapBr = sc.broadcast(modelMap)
    val classifyDict = Bayes.initGrepDicts("/home/QQ/mlearning/classify")
    val classifyDictBr = sc.broadcast(classifyDict)
    val stopWords = Bayes.getStopWords("/home/mlearning/dicts/stop_words_CN")
    val stopWordsBr = sc.broadcast(stopWords)
    val content = "上海易居房地产研究院日前发布的《全国35个大中城市房价收入比排行榜》显示，2015年，剔除可售型保障性住房后，全国35个大中城市房价收入比均值为10.2，与2014年相比下降0.4个点。不过全国房价收入比区域分化日渐加剧，一线大城市的数值升高，其中深圳高达27.7，领先其他城市。\n\n　　房价收入比是指房屋总价与居民家庭年收入的比值。报告指出，尽管房价收入比是一个全球通用的指标，但其合理范围却没有严格界定。根据我国的实际情况，报告认为全国房价收入比保持在6～7左右属合理区间。\n\n　　深圳买房“压力”较大\n\n　　报告指出，整体上看，35个大中城市的房价收入比呈现三个态势：第一，东、中、西地区呈现梯度递减的态势，且相差幅度较大。第二，一线城市明显整体高于二线城市，深圳、上海和北京领先，厦门、福州等东部二线城市则在逐渐赶超一线城市。第三，经济发达城市高于经济欠发达城市。一般而言，经济发达的一线城市、东部城市，产业发展好、收入水平高、人口吸引力强，外来常住人口比重大。因此，购房需求大、投资投机需求也大，购买力强劲，房价水平也较高。\n\n　　如果以房价收入比来代表购房者的压力的话，那么深圳的购房者压力较大。剔除可售型保障性住房后，深圳的这一比值高达27.7，而上年这一数值为21.7。如此迅速上升，主要是因为去年以来深圳房价经历了大涨。\n\n　　根据国家统计局的数据显示，2月份深圳新建商品住宅价格指数同比上涨57.8%，领先其他城市。在深圳市区的一些地段，过去一年来，房价基本都实现了翻番。\n\n　　来自江西的张小姐在一家深圳一家地产公司工作。去年初看到房价上涨，赶紧入手了一套新房，当时单价为2.8万每平方米左右，如今已经接近了六万。“现在关外很多区域的二手房都要去到五万了。现在新进入的外来大学生仅凭自己努力的话，要在深圳买房可能性已经很小了。”\n\n　　这也正是人们担心的“挤出效应”，即深圳房价过高、上涨过快，不仅加大了居民通过市场解决住房问题的难度，也对深圳人才吸引力和产业转型发展形成挤压。今年深圳“两会”上，深圳市人大代表、在科技领域工作近20年的李继朝说，如果年轻人的居住问题得不到解决，深圳对优秀人才的吸引力就会下降，从而导致深圳高科技企业的竞争力下降。\n\n　　实际上，有同样的担忧的不止深圳，同为首批经济特区的厦门，虽然经济总量仅位列全国第51，收入水平也跟北上广深有较大差距，但房价却高居全国第四，房价收入比仅次于深上北，位居全国第四，为16.6。\n\n　　厦门当地一家外贸企业工作人员谢小姐说，厦门外贸出口占据福建半壁江山，外贸出口企业众多，但很多大学生在外贸企业里工资工作几年也才五六千元，工资水平和广深有很大的差距。但厦门的房价已经超过广州，目前厦门岛内的新盘基本都在四五万以上了。\n\n　　厦门大学经济学系副教授丁长发对《第一财经日报》分析，目前厦门的房价已经过高，远远超越了其城市的产业发展和收入水平，过高的房价反过来会影响到城市的产业结构，不利于人才的流入和实体产业的发展。\n\n　　实际上，包括深圳、上海、北京、厦门等城市在内，这些城市的房价在某种程度上与本地的工资水平的关联度已经很低，而与这个城市对周围的辐射能力及汇集财富的能力联系更为紧密。\n\n　　合富房地产经济研究院院长龙斌说，在快速城市化过程中，大城市的房价是由中等偏上、偏上的人群决定的。大城市的发展空间、新供用地已经很少，但富裕人群还是不断集中，所以房价也就比较高。\n\n　　以厦门为例，易居房地产研究院的报告显示，去年厦门市商品住宅的购房人群中，本地客群购房面积为117.17万平方米，占了33.45%。而厦门以外的外地客群，总共购买了20501套商品住宅，面积233.14万平方米，占了66.45%。也就是说，去年厦门近七成的房子是被外地人买走。\n\n　　在当地业内人士看来，厦门的房价与厦门本土工资水平的关联度不是很高，因为厦门接近七成的房子被外地人买走，福建尤其是泉州等地的经商群体十分庞大，他们中的大多数都会选择到厦门置业。因此厦门的房价与福建经商群体的关联度更高。\n\n　　哪些城市买房压力小?\n\n　　与深上北厦的“宝宝们心里苦”，一些城市的买房压力要小很多。 比如，同为一线城市，广州房价收入比虽然达到了11.1，但比深上北要低太多，甚至远不如厦门、福州等二线城市，仅位列全国第十。\n\n　　这是因为自2015年以来，深圳、上海等城市房价大涨，而同样身为一线城市的广州却一直比较理性，不仅涨幅远逊于深上北，甚至比起厦门、南京、苏州等二线城市，广州的涨幅也逊色不少。目前广州的房价仅为其他三个一线城市的一半左右，甚至也不如厦门等二线城市。\n\n　　房价的背后其实是城市的实力的此消彼长。比如，在金融业增加值方面，上海、北京和深圳遥遥领先，深圳和北京的高科技实力均非常雄厚，而广州在这两方面均表现平平。\n\n　　产业发展水平是一方面，供应量是另一个方面。在土地面积上，深圳面积是1996.85平方公里，而广州则为7434平方公里。广州的人口是其1.2倍。但广州的土地面积，却是深圳的3.7倍。与深圳土地存量已接近枯竭相比，广州土地供应充足。目前广州的郊区盘如黄埔、番禺等地都有大量的一手房供应。\n\n　　由于特殊的财政体制所限，尽管广州GDP排名全国第三，但广州的地方一般公共预算收入仅位列全国第七。正是因为要补充财政收入，所以多年来广州卖地比较积极。大量的土地供应之下，广州的楼市库存去化周期一直比较长，广州的房价也十分平稳。\n\n　　在广州一家基金公司上班的程小姐说，身边很多外来大学生在工作五六年左右会考虑买房，他们的首付预算一般在40到80万之间，选择的余地还是挺大的。“如果是在深圳，想都别想了。”\n\n　　在今年2月的广州“两会”上，广州市人大代表谢小能认为，对于年轻的创新人才而言，广州目前的房价水平对于吸引他们来穗有较大的好处，“控制好房价将有利于广州城市的长远发展。我对广州的发展很有信心。”\n\n　　当然，买房压力比广州小的地方还多着去了，尤其是中西部省会城市，房价收入比值大多在10以下。这其中，近年来经济增速高速增长的“星城”长沙，这一数据仅为5.2，在35个城市中垫底，让北上深厦的“宝宝们”羡慕不已。\n\n　　中国指数研究院发布的2016年3月“百城价格指数”显示，长沙新建住宅均价为6336元/平米，这个数据甚至还不如福建浙江的很多县城的房价。就工资水平而言，长沙的工资水平不比厦门少多少，但房价只有厦门的1/4,基本上普通白领干一个月就可以买1平米。\n\n　　市场分析认为，长沙的房价低，其中的主要原因在于，长沙是个消费型城市，与浙江福建等地人喜欢置业相比，长沙更倾向于消费，因此长沙的娱乐产业十分发达，能够支撑高房价的居民储蓄水平有限。此外，一直以来长沙的单位自建房特别多，土地供应量一直很大。\n\n　　不过，低房价并不意味着长沙经济发展缓慢。数据显示，2006年长沙GDP总量仅位列全国第28，但到2015年，长沙GDP已跃居全国各大城市第14位，9年间上升了14位。这其中当地主打产业装备制造业、文化产业、医药、汽车等做出了相当大的贡献。以装备制造业为例，近年来长沙涌现出了三一重工（600031）、中联、山河智能（002097）等在国内响当当的装备制造企业。\n\n　　易居智库研究中心总监严跃进认为，实体制造产业发展得很好，城市经济对房地产的依赖度也就降低，从而进入到一个良性循环。相反，房价过高一方面会带来地价的上涨，从而抬高制造业用地成本。另一方面，制造业需要大量的生产工人，过高的房价必然带来房租乃至整体生活成本的上升，这对制造业的发展十分不利。"
    val contentFormat = TextPreprocessing.process(content, stopWordsBr.value, WordSeg.LOCAL)
    val indusString = Bayes.predict(contentFormat, modelMapBr.value, classifyDictBr.value)
    println(indusString)
  }
}
