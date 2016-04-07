/**
  * Created by QQ on 2016/4/7.
  */

import com.kunyandata.nlpsuit.util.BetterChiSqSelector
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.regression.LabeledPoint

object BetterChiTest extends App{



  new feature.IDF(minDoc).fit(trainTFRDD.map(line => {line._2}))
  val idfTemp = parasDoc.map(minDoc => {
    val idfModel =
    val labeledTrainTfIdf = trainTFRDD.map( line => {
      val temp = idfModel.transform(line._2)
      LabeledPoint(line._1, temp)
    })
    (minDoc, labeledTrainTfIdf, idfModel)
  })

  // 优化过的卡方检验选择器
  idfTemp.map(labeledTfidfTuple => {
    val betterChiSqTest = new BetterChiSqSelector()
    val ChiSqTestArray = betterChiSqTest.preFit(labeledTfidfTuple._2)
    val tempResult = parasFeatrues.map(numTopFeatrues => {
      val chiSqTestModel = betterChiSqTest.fit(ChiSqTestArray, numTopFeatrues)
      val selectedTrain = labeledTfidfTuple._2.map(line => {
        val temp = chiSqTestModel.transform(line.features)
        LabeledPoint(line.label, temp)
      })
}
