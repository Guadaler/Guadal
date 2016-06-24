//package sentiment
//
//import java.io.{PrintWriter, File}
//
//import com.kunyandata.nlpsuit.sentiment.PredictWithNb
//import com.kunyandata.nlpsuit.util.TextPreprocessing
//import org.apache.spark.mllib.classification.NaiveBayesModel
//import org.apache.spark.mllib.feature.{HashingTF, IDFModel, ChiSqSelectorModel}
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.io.Source
//
///**
//  * Created by zhangxin on 2016/4/27.
//  * 情感预测 测试主程序
//  */
//object RunPredict {
//
//  def main(args: Array[String]) {
//
//    //spark
//    val conf = new SparkConf().setAppName("Predict").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    //模型
//    val modelData=Array(
//      "D:\\000_DATA\\Model\\【第七次标注】\\2",
//      "D:\\000_DATA\\Model\\【第五次标注】\\F_2_feature_2000",
//      "D:\\000_DATA\\Model\\8【第八次标注】\\Feature_1500",
//      "D:\\000_DATA\\Model\\8【第八次标注】\\Feature_2000"
//    )
//
//    //测试数据：数据集在一个目录中，注意训练集格式，如果是一个txt文件，应调用util中的writer2local方法
//    val testData=Array(
//      "D:\\000_DATA\\predictTest\\【第五次标注】tagging",
//      "D:\\000_DATA\\predictTest\\【第五次标注】tagging",
//      "D:\\000_DATA\\predictTest\\【第七次标注】tagging（QQ_3300）",
//      "D:\\000_DATA\\predictTest\\dzh_neg_3_title\\neg",
//      "D:\\000_DATA\\predictTest\\dzh_neg_3_title\\2 pre_neg_100",
//      "D:\\000_DATA\\predictTest\\new_liumiao\\new_liumiao.txt"    //用刘妙给的数据进行测试，此数据要转一下
//    )
//
//    //预测结果写出
//    val outPath=Array(
//      "D:\\000_DATA\\predictTest\\dzh_neg_3_title\\result(8)",
//      "D:\\000_DATA\\predictTest\\dzh_neg_3_title\\2 pre_result_100",
//      "D:\\000_DATA\\predictTest\\new_liumiao\\result"  //用刘妙给的数据进行测试
//    )
//
//    val models = PredictWithNb.init(modelData(0))
//
//    //预测
//    testPredict(sc,testData(0),models)
//
//  }
//
//  /**
//    * 用未标注数据进行测试，并写出
//    * @param sc spark
//    * @param testData 测试数据集
//    * @param out 输出标注集
//    * @param models 模型
//    * @author zhangxin
//    */
//  def testPredict(sc: SparkContext, testData: String, out: String, models: Map[String, Any]): Unit = {
//
//    val stopWords = sc.textFile("D:\\111_DATA\\data\\stop_words_CN").collect()
//    val fileList = new File(testData).listFiles()
//
//    fileList.foreach(file => {
//
//      val content = Source.fromFile(file).getLines().map(line =>{line}).mkString
//      val result = PredictWithNb.predict(content, models, stopWords)
//      result match {
//        case "neg" => {
//          val wr = new PrintWriter(out + "\\neg\\" + file.getName, "utf-8")
//          wr.write(content)
//          wr.close()
//        }
//        case "neu_pos" => {
//          val wr = new PrintWriter(out + "\\neu\\" + file.getName, "utf-8")
//          wr.write(content)
//          wr.close()
//        }
//      }
//
//    })
//  }
//
//  /**
//    * 用标注数据进行测试，计算预测准确率
//    * @param sc
//    * @param testData
//    * @param models
//    */
//  def testPredict(sc: SparkContext, testData: String, models: Map[String, Any]): Unit = {
//
//    val stopWords = sc.textFile("D:\\111_DATA\\data\\stop_words_CN").collect()
//    val filemap = Util.readFile2HashMap(testData)
//    val countMap=Map( "countR"->0, "countNeg"->0, "countNegR"->0, "countNeu"->0, "countNeuR"->0)
//
//    val it = filemap.keySet().iterator()
//    while (it.hasNext) {
//
//      val file = it.next()
//      val label = file.getParentFile.getName match {
//        case "neg" => 1.0
//        case "neu" => 4.0
//      }
//
//      val content = filemap.get(file).toString
//      val result = PredictWithNb.predict(content, models, stopWords)
//
//      //如果result=label，则预测正确
//      if (result == label) {
//
//        countMap("countR") +=1
//        result match {
//          case "neg" => {
//            countMap("countNeg") += 1
//            countMap("countNegR") += 1
//          }
//          case "neu_pos" => {
//            countMap("countNeu") += 1
//            countMap("countNeuR") += 1
//          }
//        }
//
//      } else {
//
//        label match {
//          case "neg" => {
//            countMap("countNeg") += 1
//          }
//          case "neu_pos" => {
//            countMap("countNeu") += 1
//          }
//        }
//
//      }
//    }
//
//    println("总数为：" + filemap.size() + "   正确个数为：" + countMap("countR"))
//    println("平均正确率为：" + countMap("countR").toDouble / filemap.size().toDouble)
//    println("neg正确率：" + (countMap("countNegR").toDouble / countMap("countNeg").toDouble))
//    println("neu正确率：" + (countMap("countNeuR").toDouble / countMap("countNeu").toDouble))
//  }
//
//}
