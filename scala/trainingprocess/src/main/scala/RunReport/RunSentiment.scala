package RunReport

import java.io.PrintWriter
import java.sql.{ResultSet, DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by zhangxin on 2016/6/17.
  *
  * 情感分析运行报告主程序
  */
object RunSentiment {
  def main(args: Array[String]) {
    val conn = getConn()
    val str = "select id ,title,sentiment,summary from news.news_info where sentiment!=-1 ORDER BY id DESC LIMIT 50 "
    val st = conn.createStatement()
    val rs = st.executeQuery(str)

    //输出到文件
    write2MD(rs)
  }

  /**
    * 将运行情况写入到md文件
    * @param rs
    */
  def write2MD(rs: ResultSet): Unit = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val time = dateFormat.format( now )
    val txt = "C:\\Users\\Administrator.HTBJUOLFUL4H7CK\\Desktop\\模型运行报告\\模型运行报告_"+time+".md"
    println(txt)

    val writer = new PrintWriter(txt,"utf-8")
    //程序运行报告
    writer.append("# 情感分析运行报告 #\n")
    val time2 = getNowDate()
    writer.append(time2 + "\n")
    writer.append("  情感分析程序稳定运行，没有异常和中断。\n")

    //程序分析结果
    writer.append("----------\n")
    writer.append("## 情感分析结果抽样 ##\n")
    writer.append("### 随机抽取今天50篇文本实际情况 ###\n")
    writer.append("|类别|实际|\n|:----:|:---:|\n")
    writer.append("|利空|00|\n")
    writer.append("|利好|00|\n")

    writer.append("### 预测统计结果 ###\n")
    writer.append("|预测类别|计数| 正确 | 错误 | 准确率 | 召回率 |\n|:----:|:---:|:-----:|:-----:|\n")
    writer.append("|预测利空|00|00|00|00|00|\n")
    writer.append("|预测利好|00|00|00|00|00|\n")

    //输出附录
    writer.append("## 附录 ##\n")
    writer.append("### 随机抽取今天50例样本 ###\n")
    writer.append("|实际|预测|文章id|文章标题|文章内容|\n")
    writer.append("|:---:|:---:|:----:|:---:|:---:|\n")
    while(rs.next()){
      val newsId = rs.getInt(1)
      val title = rs.getString(2).replace("|", ":")
      val sentiment = rs.getInt(3)
      val summery = rs.getString(4)

      print("【" + newsId + "】  【" + title + "】  " + summery + "\n\n")

      writer.append("|"+"|" + sentiment + "|" + newsId + "|" + title + "||\n")
      writer.flush()
    }
    writer.flush()
    writer.close()
  }


  /**
    * 获取MySQL连接
    * @return
    */
  def getConn(): Connection ={
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://222.73.57.12:3306/stock"
    val username = "news"
    val password = "news"
    var connection: Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    }catch {
      case e => e.printStackTrace
    }
    connection
  }

  /**
    * 获取当前的系统时间
    * @return
    */
  def getNowDate():String={
    val now: Date = new Date()
    val  dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val hehe = dateFormat.format( now )
    hehe
  }
}
