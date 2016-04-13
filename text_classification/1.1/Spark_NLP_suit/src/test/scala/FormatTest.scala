/**
  * Created by QQ on 2016/4/12.
  */

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

/**
  * Created by root on 4/13/16.
  */
class FormatTest extends  FlatSpec with Matchers {

  "test " should "work" in{

    println(System.getProperty("user.dir"))
    val text = Source.fromFile("/home/ParasResult/new 3.txt").getLines().toArray
    text.foreach(line => {
    val a = line.replaceAll("<[^<]*>", "").replaceAll("&nbsp", "")
    println(a)
    })
  }
}
