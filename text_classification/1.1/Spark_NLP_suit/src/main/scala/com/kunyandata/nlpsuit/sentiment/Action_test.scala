package com.kunyandata.nlpsuit.sentiment

/**
  * Created by Zx on 2016/3/17.
  */
object Action_test {
      def main(args: Array[String]) {

          //测试Map遍历
  //        var map=new util.HashMap[Int,String]
  //        map.put(1,"Rice")
  //        map.put(2,"Guadal")
  //        map.put(3,"Kana")
  //
  //        var it=map.keySet().iterator()
  //        while(it.hasNext){
  //            var key=it.next()
  //            println(key+" "+map.get(key))
  //        }

          //测试去掉空格，，但是为什么程序里面去不掉！！怪
          /*var s=" hellow nihao woshi "
          var str=s.trim()
          var str2=s.replace(" ","")
          println(s+s.indexOf(" "))
          println(str+str.indexOf(" "))
          println(str2+str2.indexOf(" "))*/

          //测试浮点数
         /* var a:Double=0.000;
          a=0.005
          println(a)

          var b:Double=3.00/4.00
          println(b)
          println(3.00/4.00)*/

        //测试list
        /*var keys=List[Int]()
        keys.::(2)
        keys.::(1)
        keys.::(9)
        keys.::(8)
        keys.sorted
        println(keys)*/

        //test sort
        /*val lb =ListBuffer[Int]()
        lb.append(2)
        lb.append(1)
        lb.append(8)
        lb.append(6)
        var la=lb.sorted
        println(la)*/

        // Scala教程  readLine和readInt
        /*var name=readLine("Your name:")
        print("Your age:")
        var age=readInt()
        printf("Hello,%s!,Next year,you will be %d old",name,age+1)*/

        /*for(i <-1 to 3;j <- 1 to 3 if i != j){
            println("i:"+i+"  j:"+j)
        }

        for(i <-1 to 3;j <- 1 to 3 if i == j){
          println("i:"+i+"  j:"+j)
        }

        println(decorata("Hello"))
        println(decorata("Hello","<<<",">>>"))
        println(decorata("Hello","<<<"))
        println(decorata(left="<<<",str="Hello",right=">>>"))

        box(" Hello,Word！")
      }

      def decorata(str:String,left:String="[",right:String="]")={
          left+str+right
      }

      def box(s:String){
          var border="-"*s.length+"--\n"
          println(border+"|"+s+"|\n"+border)
      }*/
      /*var s="abc。123，456。999，haha";
      var ss=s.split("，|。")
      for(a <-Range(0,ss.length)){
        println(ss(a));
      }

        var str="2015年底中国煤制乙二醇产能预计达300万吨/年";
        str.replace("/","每")
        println(str)*/

        /*var myVar = "theValue";
        var myResult =
          myVar match {
            case "someValue"   => myVar + " A";
            case "thisValue"   => myVar + " B";
            case "theValue"    => myVar + " C";
            case "doubleValue" => myVar + " D";
          }
        println(myResult);*/

        /*var a=2000
        var b=1815
        println((b/a)+1)*/
}}
