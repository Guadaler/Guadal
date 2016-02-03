package Title_senti

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.json.JSONObject
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisPool}

import scala.collection.mutable

/**
  * Created by Liu on 2016/1/19.
  */

object title_senti {
  /* this will count news emotional tendency
   * and count every classify (industry  stock  section ) positive negative and neutral titles' percents
   */
  def main(args: Array[String]) {
    /* this is the main function
     * create spark object
     */

    val conf = new SparkConf().setAppName("News_title_sentiment_lm")
//      .setMaster("local")
    val sc = new SparkContext(conf)

    // add user defined dictionary (use to cut news)
//    add_userdic(sc, "C:\\Users\\Administrator\\IdeaProjects\\Test\\src\\Title_senti\\user_dict.txt")
    add_userdic(sc, args(0))

    // read dicts
    val posi_dic = read_dic(sc, args(1))
    val nega_dic = read_dic(sc, args(2))
    val f_dic = read_dic(sc, args(3))

    // connect to redis
    val redis = get_redis(sc, args(4))
    // get all table's name
    val time = get_sys_time()
//    val time = "20160129"
    val ind_time = "Industry_" + time                            // ------------------------------- industry -----------------------------
    val sto_time = "Stock_" + time                               // ------------------------------- stock --------------------------------
    val sec_time = "Section_" + time                             // ------------------------------- section ------------------------------
    val key_time = "News_" + time                                // -------------------------------- news --------------------------------

    // count percents
    val list_1 = count_percents(redis,ind_time,key_time,posi_dic,nega_dic,f_dic)
    val list_2 = count_percents(redis,sto_time,key_time,posi_dic,nega_dic,f_dic)
    val list_3 = count_percents(redis,sec_time,key_time,posi_dic,nega_dic,f_dic)

    //write to Mysql
    write_Myaql(sc, args(5), "industry_senti_tend", "indus", list_1)
    write_Myaql(sc, args(5), "stock_senti_tend", "stock", list_2)
    write_Myaql(sc, args(5), "section_senti_tend", "secti", list_3)
  }

  def read_dic(sc:SparkContext, file:String):Array[String] = {
    /* this will read dictionary from the text document
     * and store to an array[string]
     */
    // read dict from text to Array
    val dic = sc.textFile(file).collect()
    dic
  }

  def add_userdic(sc:SparkContext, file:String): Unit ={
    /* this add user defined dictionary to the Ansj
     * make it cut right words
     */
    val dic = sc.textFile(file).collect()
    for(x<-dic){
      // add new words
      UserDefineLibrary.insertWord(x,"userDefine",100)
    }
  }

  def get_date(): String ={
    /* this can get the system's time
     * and return a string like "yyyy-MM-dd HH:mm:ss"
     */
    val now = new Date()
    var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val out = dateFormat.format(now)
    out
  }

  def cut(sentence:String):Array[String] = {
    /* this will cut the sentence to words
     * filter the POS tagging with the words and store to an array[string]
     */
    // cut sentence
    val sent = ToAnalysis.parse(sentence)
   // filter the POS tagging
    val words = for(i <- Range(0,sent.size())) yield sent.get(i).getName
    val result = new Array[String](sent.size())
    // change Vector to Array
    words.copyToArray(result)
    result
  }

  def count_senti(i:Int, sentence:Array[String], dic:Array[String]): Int ={
    /* this will search neg word nearby the sentiment word
     * if find a neg word, reverse the sentiment word's emotional tendency
     */
    // find neg word before sentiment word
    if (i-1 > 0){
      if (dic.contains(sentence(i-1))){
        return -1
      }
      else if (i-2 >0){
        if (dic.contains(sentence(i-2))){
          return  -1
        }
      }
    }
    // fine neg word behind sentiment word
    if (i+1 < sentence.length){
      if(dic.contains(sentence(i+1))){
        return -1
      }
      else if(i+2 < sentence.length){
        if (dic.contains(sentence(i+2))){
          return -1
        }
      }
    }
    // with no neg word return 1
    1
  }

  def search_senti(title_cut:Array[String], dict_p:Array[String], dict_n:Array[String], dict_f:Array[String], writer: PrintWriter): Int ={
    /* this will search the sentiment words in the sentence
     * count the sentence emotional tendency according to the sentiment word
     */
    // Interrogative Sentences
//    if (title_cut(title_cut.length-1) == "?"){
//      return 0
//    }
    var p = 0
    var n = 0
    var s = ""
    // traverse every word in sentence
    for (i <- Range(0,title_cut.length)) {
      val t_c = title_cut(i)
      // if word in positive dictionary
      if(dict_p.contains(t_c)){
        if(count_senti(i, title_cut, dict_f)>0){
          p = p + 1
        }
        else{
          n = n + 1
        }
        s = s + t_c + " "
      }
      // if word in negative dictionary
      else if (dict_n.contains(t_c)){
        if(count_senti(i, title_cut, dict_f)>0){
          n = n + 1
        }
        else{
          p = p + 1
        }
        s = s + t_c + " "
      }
    }
    // positive
    if (p > n){
      writer.write("，" + s + "，" + "positive" + "\n")
      1
    }
    // negative
    else if (p < n){
      writer.write("，" + s + "，" + "negative" + "\n")
       -1
    }
    // neutral
    else{
      writer.write("，" + "NULL" + "，" + "neutral" + "\n")
      0
    }
  }

  def get_redis(sc:SparkContext, file:String): Jedis ={
    /* connect to redis
     * get redis resource
     */
    // get redis info
    val info = sc.textFile(file).collect()
    // set the parameters
    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)
    // set the redis Host port password and database
    val redisHost = info(0)
    val redisPort = info(1).toInt
    val redisTimeout = 30000
    val redisPassword = info(2)
    val redisDatabase = info(3).toInt
    // connect
    val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, redisPassword, redisDatabase)
    val jedis = pool.getResource()
    pool.close()
    jedis
  }

  def json(title:String):JSONObject = {
    /* this can change string to json
     * the info store in redis is json
     */
    val t = new JSONObject(title)
    t
  }

  def write_Myaql(sc:SparkContext, file:String, db:String, classify:String, list:mutable.MutableList[Tuple8[String,String,Int,Int,Int,Float,Float,Float]]): Unit = {
    /* this can connect to MySQL
     * and write recode to table
     */
    // get MySQL info
    val info = sc.textFile(file).collect()
    val  MySql = info(0)
    // create SQLContext
    val sqlContent = new SQLContext(sc)
    // create scheam
    val scheam =
      StructType(
        StructField(classify, StringType, true) ::
          StructField("day_time", StringType, true) ::
          StructField("p_num", IntegerType, true) ::
          StructField("n_num", IntegerType, true) ::
          StructField("o_num", IntegerType, true) ::
          StructField("p_percent", FloatType, true) ::
          StructField("n_percent", FloatType, true) ::
          StructField("o_percent", FloatType, true) :: Nil)
    // write date to table "titles_tend"
    val data = sc.parallelize(list).map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8))
//    sqlContent.createDataFrame(data,scheam).write.mode("append").jdbc("jdbc:mysql://112.124.49.59:3306/stock?user=migfm&password=miglab2012&useUnicode=true&characterEncoding=utf8","titles_tend",new Properties())
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sqlContent.createDataFrame(data,scheam).write.mode("append").jdbc(MySql,db,properties)
  }

  def get_sys_time(): String ={
    /* this can get the system's time
     * and return a string like "HH-mmss"
     */
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val out = dateFormat.format(now)
    out
  }

  def count_percents(redis:Jedis, db_name:String, db_news:String, posi_dic:Array[String], nega_dic:Array[String], f_dic:Array[String]):
  mutable.MutableList[Tuple8[String,String,Int,Int,Int,Float,Float,Float]] ={
    /* this can count different industry's positive negative and neutral news' percent
     * and return a list that stored "industry  date  po_num  ne_num  ne_num  po_percent  ne_percent  nu_percent"
     */
    // get all classify information and news' code
    val writer = new PrintWriter(new File("n_1.txt" ))
    val s = redis.hkeys(db_name)                           // get classify name
    val c = redis.hkeys(db_news)                           // get all news code
    val classify = new  Array[String](s.size())
    val code = new Array[String](c.size())
    s.toArray(classify)
    c.toArray(code)
    // create a list and get system time
    val list = new mutable.MutableList[Tuple8[String,String,Int,Int,Int,Float,Float,Float]]
    val date = get_date()
    // for every industry count news tendency percent
    for (i <- Range(0,classify.length)) {
      var p = 0
      var n = 0
      var m = 0
      var sum = 0.0f
      // get every industry's all news code
      val ss = redis.hget(db_name, classify(i))
      // print classify name
      writer.write(classify(i) + "\n")
      val news = ss.split(",")
      // for every new get it's title info
      for (j <- Range(0, news.length)) {
        //        if(code.contains(news(j))){
        val all = redis.hget(db_news, news(j))
        // get title
        val t = json(all).getString("title")
        // count title's emotional tendency
        val title_cut = cut(t)
        // print title cut content
        for (i <- Range(0, title_cut.length)) {
          writer.write(title_cut(i).toString() + " ")
        }
        val value = search_senti(title_cut, posi_dic, nega_dic, f_dic, writer)
        if (value > 0) {
          p = p + 1
        }
        else if (value < 0) {
          n = n + 1
        }
        else {
          m = m + 1
        }
        //      }
      }
      sum = p + n + m
      println(classify(i) + " " + p + " " + n + " " + m)
//      val df = new DecimalFormat("#.00")
//      val p_c = df.format(p/sum)
      list.+=((classify(i), date, p, n, m, p/sum, n/sum, m/sum))

      // print \n to distinguish different classify
      writer.write( p.toString + "  " + n.toString + "  " + m.toString + "\n\n\n")
    }
    writer.close()
    list
  }

}
