package LDA

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Created by zhangxin on 2016/5/18.
  * LDA训练：EM模式 和 Online模式
  */
object Train {

  /**
    * 基于EM模型训练
    * @param sc sc
    * @param args 模型参数
    * @param countVectors 训练集
    * @return DistributedLDAModel
    * @author zhangxin
    */
  def trainWithEM(sc:SparkContext, args:Array[Int],countVectors:RDD[(Long,Vector)]): LDAModel ={
    val ldaModel=new LDA()
      .setK(args(0))
      .setMaxIterations(args(1))
      .setDocConcentration(args(2))
      .setTopicConcentration(args(3))
      .setSeed(0L)
      .setCheckpointInterval(10)
      . setOptimizer("em")
      .run(countVectors)
      .asInstanceOf[DistributedLDAModel]

    ldaModel
  }

  /**
    * 基于Online模式的训练
    * @param sc sc
    * @param args 模型参数
    * @param countVectors 训练集
    * @return LocalLDAModel
    * @author zhangxin
    */
  def trainWithOnline(sc:SparkContext, args:Array[Int], countVectors:RDD[(Long,Vector)]): LDAModel ={

    val mbf =2.0 / args(1) + 1.0 / countVectors.count()

    val miniBatchFraction1=0.8
    val miniBatchFraction2=math.min(1.0, mbf)

    val optimizer=new OnlineLDAOptimizer().setMiniBatchFraction(miniBatchFraction2)

    val ldaModel = new LDA()
      .setK(args(0))
      .setMaxIterations(args(1))
      .setDocConcentration(args(2))
      .setTopicConcentration(args(3))
      .setOptimizer(optimizer)
      .run(countVectors)
      .asInstanceOf[LocalLDAModel]

    ldaModel
  }
}
