//mainClass in (Compile, packageBin) := Some("com.kunyandata.nlp.classification.TrainingProcess")

name := "Spark_NLP_suit"

version := "0.2"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "3.1.14" % "provided"

libraryDependencies += "org.ansj" % "ansj_seg" % "0.9" % "provided"

libraryDependencies += "org.json" % "json" % "20160212" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2" exclude("org.apache.spark", "spark-streaming_2.10")

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.3" % "provided"

//libraryDependencies += "org.apache.hbase" % "hbase" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2" % "provided"

libraryDependencies +="org.apache.hbase" % "hbase-server" % "1.1.2" % "provided"

libraryDependencies += "com.ibm.icu" % "icu4j" % "56.1" % "provided"

//libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.2"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}

