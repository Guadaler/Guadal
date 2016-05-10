package sentiment

import classification.TrainWithNb
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 0
  * Created by Administrator on 2016/4/22.
  */
object TestTrain {
  def main(args: Array[String]) {
    val sconf=new SparkConf().setAppName("training").setMaster("local")
    val sc=new SparkContext(sconf)
//    val filepath="D:\\000_DATA\\out\\【第六次标注程序结果】\\QQ_3900.txt"
//    val filepath="D:\\000_DATA\\out\\【第五次标注程序结果】\\F_1700_textSeg_content_2.txt"
//    val filepath="D:\\000_DATA\\out\\【第七次标注程序结果】\\QQ_3300.txt"
    val filepath="D:\\000_DATA\\out\\8【第八次】\\QQ_zx_3400.txt"

//    "D:\\000_DATA\\Model\\8【第八次标注】\\Feature_2000"
    val modelout="D:\\000_DATA\\Model\\8【第八次标注】"
    TrainWithNb.nbTrainToLocal(sc,filepath,modelout,"model_qq3900",0,2000)
//    TrainWithNb.nbTrain(sc,filepath)
  }
}
