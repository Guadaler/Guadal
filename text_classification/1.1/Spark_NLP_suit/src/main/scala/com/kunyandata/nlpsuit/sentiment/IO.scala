package com.kunyandata.nlpsuit.sentiment

import java.io._
import java.util

import scala.io.Source

/**
  * Created by zx on 2016/3/16.
  */
object IO {
    def main(args: Array[String]) {
      var file = "src/main/resource/copy.txt"
      for (line <- Source.fromFile(file).getLines)
        println(line.length + " " + line)

//      var file2 = new File("src/main/resource/copy.txt")
//      var bufferdSource = Source.fromFile(file2)
//      bufferdSource.getLines()

      //test read file
//      readfile_test("src/main/resource")

      //test scala map
      println("[测试Map]")
      var treasureMap =Map[Int, String]()
      treasureMap += (1->"Go to island.")
      treasureMap +=(2 -> "Find big X on ground.")
      treasureMap +=(3 -> "Dig.")
      println(treasureMap.size+" "+treasureMap.get(2))
      for(key <-treasureMap.keys){
          println(key+"  "+treasureMap.get(key))
      }

      //测试文件读取
      var path="src/main/resource/sourcedata"
      var file_map:Map[File,String]=readfile(path)
      var content:String=""
      for(file <-file_map.keys){
          println(file.getName()+"  "+file_map(file))
          content +=file_map(file)+"\n"
      }

      //测试写入
      var outpath="src/main/resource/copy.txt"
      writefile(outpath,content)
    }

    /**
      * 读取文件获得文章
      * @param path  文件父目录的路径
      * @return  所有文章map[File,String] ： File 文章对象  String 文章内容
      */
    def readfile(path:String): Map[File,String]={
      var file_map = Map[File,String]()
      var files=new File(path).listFiles()   //获取父目录文件列表

      for(file <-files){
          println(file.getAbsoluteFile)
          var str:String=""
          for(line <-Source.fromFile(file).getLines()){
              str +=line
          }
        file_map +=(file ->str)
      }
      println("Read Over!")
      file_map
    }

    //用HashMap
    def readfile2(path:String): util.HashMap[File,String]={
      var file_map =new util.HashMap[File,String]()
      var catDir=new File(path).listFiles()   //获取父目录文件列表
      for(dir <-catDir){
        var files=dir.listFiles()
        println(dir+"   共 "+files.length+" 篇")
        for(file <-files){
              var str=""
              for(line <-Source.fromFile(file).getLines()){
                str +=line
              }
              file_map.put(file,str)
          }
      }
      println("Read Over!")
      file_map
    }
  /**
    * 写入文件
    * @param outpath  写入文件路径
    * @param content  写入文件内容
    */
    def writefile(outpath:String,content:String): Unit ={
        var writer=new PrintWriter(new File(outpath),"UTF-8")
        writer.write(content)
        writer.flush()
        writer.close()
//        println("Write Over!")
    }


}
