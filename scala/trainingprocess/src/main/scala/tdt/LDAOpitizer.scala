package tdt

import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.mllib.linalg.Matrix

/**
  * Created by zhangxin on 2016/5/10.
  * 本类主要用于LDA模型优化 参数K优化
  */
object LDAOpitizer {

  //计算topics平均相似度
  /**
    * 计算已训练好的LDA模型的主题平均相似度
    * @param ldaModel  已训练好的LDA模型
    * @param k 主题数量
    * @return 该模型的平均相似度
    */
  def calculateAverageSimilar(ldaModel:LDAModel, k:Int): Double ={
    val topics: Matrix = ldaModel.topicsMatrix
    var sum=0.0
    for(i <-Range(0,k)){
      var fenzi=0.0
      var fenmu=0.0
      var x=0.0
      var y=0.0
      for(j <-Range(i+1,k)){
        for(word <- Range(0,ldaModel.vocabSize)){
          val topic_i=topics(word,i)
          val topic_j=topics(word,j)
          fenzi +=topic_i*topic_j

          x +=scala.math.pow(topic_i,2)
          y +=scala.math.pow(topic_j,2)
        }
        fenmu=scala.math.sqrt(x*y)
        var similar=fenzi/fenmu
        println("【Topic"+i+" _ Topic "+j+"】  :  "+similar)
        sum +=similar
      }
    }

    val averageSimilar=2*sum/(k*(k-1))

    averageSimilar
  }
}
