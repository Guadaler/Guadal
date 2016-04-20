import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.regression.LabeledPoint
import scala.io.Source


/**
  * Created by QQ on 2016/4/17.
  */
object MultiClassTest extends App {

  val conf = new SparkConf().setAppName("outputModels").setMaster("local")
  val sc = new SparkContext(conf)

  val train = sc.parallelize(Source.fromFile("D:/纺织服装").getLines().toSeq).map(line => {
    val Array(label, content) = line.split("\t")
    (label.toDouble, content.split(","))
  })
  val VSMlength = countWords(train)
  val hashingTFModel = new feature.HashingTF(VSMlength)
  val trainTFRDD = train.map(line => {
    val temp = hashingTFModel.transform(line._2)
    (line._1, temp)
  })
  // 计算idf
  val idfModel = new feature.IDF(2).fit(trainTFRDD.map(line => {line._2}))
  val labeedTrainTfIdf = trainTFRDD.map( line => {
    val temp = idfModel.transform(line._2)
    LabeledPoint(line._1, temp)
  })
  // 卡方降维
  val chiSqSelectorModel = new feature.ChiSqSelector(500).fit(labeedTrainTfIdf)
  val selectedTrain = labeedTrainTfIdf.map(line => {
    val temp = chiSqSelectorModel.transform(line.features)
    LabeledPoint(line.label, temp)
  })
  // 贝叶斯分类器
  val nbModel = NaiveBayes.train(selectedTrain, 1.0, "multinomial")
  val models = Map("tfModel" -> hashingTFModel, "idfModel" -> idfModel,
    "chiSqSelectorModel" -> chiSqSelectorModel, "nbModel" -> nbModel)
  val predictionAndLabels = selectedTrain.map {line =>
    val prediction = nbModel.predict(line.features)
    val probabilities = nbModel.predictProbabilities(line.features)
    //      println((prediction, line.label))
    (prediction, probabilities, line.label)
  }
  // 计算精度和召回率
  val metrics = new MulticlassMetrics(predictionAndLabels.map(line => {
    (line._1, line._3)
  }))
  predictionAndLabels.foreach(x => println(x._2))
}
