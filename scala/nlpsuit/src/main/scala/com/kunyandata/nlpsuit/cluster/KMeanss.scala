package com.kunyandata.nlpsuit.cluster

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import scala.io.Source

/**
  * Created by QQ on 5/9/16.
  */
object KMeanss {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SClusterTest")
      .setMaster("local")
      .set("spark.local.ip", "192.168.2.65")
      .set("spark.driver.host", "192.168.2.90")

    val sc = new SparkContext(conf)
    val kmeansdata = sc.parallelize(Source.fromFile("/home/QQ/Downloads/spark-1.5.2/data/mllib/kmeans_data.txt").getLines().toSeq)
    val parsedData = kmeansdata.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    println(clusters.clusterCenters.toSeq)
    clusters.predict(parsedData).foreach(println)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }

}
