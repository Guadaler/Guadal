import java.io.File
import java.util.Date
import javax.xml.parsers.DocumentBuilderFactory

import kafka.serializer.StringDecoder
import org.apache.log4j.{BasicConfigurator, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.w3c.dom.Element

import scala.collection.mutable
/**
  * Created by root on 4/15/16.
  */
object BroadcastTest extends App{

  class RootLogger {

    var logger = Logger.getLogger("NEWS_PARSER_ROOT")
    BasicConfigurator.configure()
    PropertyConfigurator.configure("/home/newsparser/conf/log4j.properties")

    var switch = true

    def exception(e: Exception) = {
      if (switch)
        e.printStackTrace()
      logger.error(e.printStackTrace())
    }

    def error(msg: String): Unit = {
      if (switch)
        logger.error(msg)
    }

    def warn(msg: String): Unit = {
      if (switch)
        logger.warn(msg)
    }

    def info(msg: String): Unit = {
      if (switch)
        logger.info(msg)
    }

    def debug(msg: String): Unit = {
      if (switch)
        logger.debug(msg)
    }

  }

  object Configuration {

    def getConfigurations(path:String): (mutable.Map[String, String], mutable.Map[String, String], mutable.Map[String, String]) = {

      val redis = mutable.Map[String, String]()
      val kafka = mutable.Map[String, String]()
      val hbase = mutable.Map[String, String]()

      val file = new File(path)

      val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file)

      //redis
      val redisRoot = doc.getElementsByTagName("redis").item(0).asInstanceOf[Element]
      val redisIp = redisRoot.getElementsByTagName("ip").item(0).getTextContent
      val redisPort = redisRoot.getElementsByTagName("port").item(0).getTextContent
      val redisDb = redisRoot.getElementsByTagName("db").item(0).getTextContent
      val redisAuth = redisRoot.getElementsByTagName("auth").item(0).getTextContent

      redis.put("ip", redisIp)
      redis.put("port", redisPort)
      redis.put("db", redisDb)
      redis.put("auth", redisAuth)

      //kafka
      val kafkaRoot = doc.getElementsByTagName("kafka").item(0).asInstanceOf[Element]
      val kafkaIp = kafkaRoot.getElementsByTagName("ip").item(0).getTextContent
      val zookeeper = kafkaRoot.getElementsByTagName("zookeeper").item(0).getTextContent
      val groupId = kafkaRoot.getElementsByTagName("groupId").item(0).getTextContent
      val urlTopic = kafkaRoot.getElementsByTagName("urlTopic").item(0).getTextContent
      val brokerList = kafkaRoot.getElementsByTagName("brokerList").item(0).getTextContent
      val taskTopic = kafkaRoot.getElementsByTagName("taskTopic").item(0).getTextContent
      val newsTopic = kafkaRoot.getElementsByTagName("newsTopic").item(0).getTextContent
      val contentTopic = kafkaRoot.getElementsByTagName("contentTopic").item(0).getTextContent

      kafka.put("ip", kafkaIp)
      kafka.put("zookeeper", kafkaIp + ":" + zookeeper)
      kafka.put("groupId", groupId)
      kafka.put("urlTopic", urlTopic)
      kafka.put("brokerList", kafkaIp + ":" + brokerList)
      kafka.put("taskTopic", taskTopic)
      kafka.put("newsTopic", newsTopic)
      kafka.put("contentTopic", contentTopic)

      //hbase
      val hbaseRoot = doc.getElementsByTagName("hbase").item(0).asInstanceOf[Element]
      val hbaseRootDir = hbaseRoot.getElementsByTagName("rootDir").item(0).getTextContent
      val hbaseIp = hbaseRoot.getElementsByTagName("ip").item(0).getTextContent

      hbase.put("rootDir", hbaseRootDir)
      hbase.put("ip", hbaseIp)

      (redis, kafka, hbase)
    }

  }

  object KafkaConfig {

    def contentTopic: String = "newsparser_contentparse"

    def taskTopic: String = "newsparser_task"

    def urlTopic: String = "newsparser_url"

    def newsTopic: String = "newsparser_news"

  }

  val sparkConf = new SparkConf()
    .setAppName("BrTest")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .setMaster("local")
    .set("spark.local.ip","192.168.2.65")
    .set("spark.driver.host","192.168.2.65")

  val ssc = new StreamingContext(sparkConf, Seconds(5))
  val configurations = Configuration.getConfigurations("/home/config_12.xml")
  val kafka = configurations._2

  val dateBr = ssc.sparkContext.broadcast(new Date().getTime)
  val topicsSet = KafkaConfig.taskTopic.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> kafka.get("brokerList").get,
    "group.id" -> kafka.get("groupId").get)

  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)


  messages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {
    rdd.foreach(x => {
      Logger.getLogger("A name").warn(dateBr.value)
    })
  })

  ssc.start()
  ssc.awaitTermination()
}
