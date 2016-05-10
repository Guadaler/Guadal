package sentiment

import java.io.{PrintWriter, File}

import com.kunyan.nlpsuit.sentiment.PredictWithNb
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
        val result = PredictWithNb.predict(temp, models, stopBr)
        result match {
          case 1.0 => {
            val wr = new PrintWriter(out + "\\neg\\" + count+".txt", "utf-8")
            wr.write(temp)
            wr.close()
          }
          case 4.0 => {
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
    val stopBr = sc.broadcast(stopWords)
    val fileList = new File(files).listFiles()
    fileList.foreach(file => {
      val content = Source.fromFile(file).getLines().map(line =>{line}).mkString
      val result = PredictWithNb.predict(content, models, stopBr)
      result match {
        case 1.0 => {
          val wr = new PrintWriter(out + "\\neg\\" + file.getName, "utf-8")
          wr.write(content)
          wr.close()
        }
        case 4.0 => {
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
      val result = PredictWithNb.predict(content, models, stopBr)
      if (result == label) {
        count += 1
        count_r += 1
        result match {
          case 1.0 => {
            count_neg += 1;
            count_neg_r += 1
          }
          case 4.0 => {
            count_neu += 1;
            count_neu_r += 1
          }
        }
      } else {
        count += 1
        result match {
          case 1.0 => {
            count_neg += 1
          }
          case 4.0 => {
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
      val result = PredictWithNb.predict(content, models, stopBr)
      if (result == label) {
        count += 1
        count_r += 1
        result match {
          case 1.0 => {
            count_neg += 1;
            count_neg_r += 1
          }
          case 4.0 => {
            count_neu += 1;
            count_neu_r += 1
          }
        }
      } else {
        count += 1
        result match {
          case 1.0 => {
            count_neg += 1
            val wr = new PrintWriter(outPath + "\\neg\\" + file.getName + ".txt", "utf-8")
            wr.write(content)
            wr.close()
          }
          case 4.0 => {
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

  def deal(): Unit ={
    val file=""
    val out=""

  }
}
