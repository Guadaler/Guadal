//package com.kunyandata.nlpsuit.topicmodel
//
///**
//  * Created by QQ on 2016/3/28.
//  */
//
//import org.apache.spark.mllib.clustering.LDA
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//
//object LDA extends App{
//
//  // 输入的文件每行用词频向量表示一篇文档
//  val conf = new SparkConf().setAppName("topicmodel").setMaster("local")
//  val sc = new SparkContext(conf)
//  val data = sc.textFile("D:/sample_lda_data.txt")
//  val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
//  val corpus = parsedData.zipWithIndex.map(_.swap).cache()
//  val ldaModel = new LDA().setK(3).run(corpus)
//
//  // 打印主题
//  println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
//  val topics = ldaModel.topicsMatrix
//  for (topic <- Range(0, 3)) {
//    print("Topic " + topic + ":")
//    for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
//    println()
//  }
//
//  import scala.collection.mutable
//  import org.apache.spark.mllib.clustering.LDA
//  import org.apache.spark.mllib.linalg.{Vector, Vectors}
//  import org.apache.spark.rdd.RDD
//
//  // Load documents from text files, 1 document per file
//  val corpus: RDD[String] = sc.wholeTextFiles("docs/*.md").map(_._2)
//
//  // Split each document into a sequence of terms (words)
//  val tokenized: RDD[Seq[String]] =
//    corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
//
//  // Choose the vocabulary.
//  //   termCounts: Sorted list of (term, termCount) pairs
//  val termCounts: Array[(String, Long)] =
//    tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
//  //   vocabArray: Chosen vocab (removing common terms)
//  val numStopwords = 20
//  val vocabArray: Array[String] =
//    termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
//  //   vocab: Map term -> term index
//  val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
//
//  // Convert documents into term count vectors
//  val documents: RDD[(Long, Vector)] =
//    tokenized.zipWithIndex.map { case (tokens, id) =>
//      val counts = new mutable.HashMap[Int, Double]()
//      tokens.foreach { term =>
//        if (vocab.contains(term)) {
//          val idx = vocab(term)
//          counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
//        }
//      }
//      (id, Vectors.sparse(vocab.size, counts.toSeq))
//    }
//
//  // Set LDA parameters
//  val numTopics = 10
//  val lda = new LDA().setK(numTopics).setMaxIterations(10)
//
//  val ldaModel = lda.run(documents)
//  val avgLogLikelihood = ldaModel.logLikelihood / documents.count()
//
//  // Print topics, showing top-weighted 10 terms for each topic.
//  val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
//  topicIndices.foreach { case (terms, termWeights) =>
//    println("TOPIC:")
//    terms.zip(termWeights).foreach { case (term, weight) =>
//      println(s"${vocabArray(term.toInt)}\t$weight")
//    }
//    println()
//  }
//
//}
