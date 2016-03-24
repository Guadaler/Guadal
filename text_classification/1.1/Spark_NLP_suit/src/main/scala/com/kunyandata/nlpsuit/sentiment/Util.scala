package com.kunyandata.nlpsuit.sentiment

import java.io.File
import java.util

/**
  * Created by zx on 2016/3/17.
  */
object Util {

  /**
    * 根据文件名提取文件类别
    *
    * @param file  文件名
    * @return  类别
    */
  def getLabel(file:File): String ={
      var parentPath=file.getParentFile();
      var label=parentPath.getName();
      label;
  }

  /**
    * 加载类别标签
    *
    * @return 返回类别标签map
    */
  def loadlabel_map(): util.HashMap[String,Int] ={
    var label_map=new util.HashMap[String,Int]
    label_map.put("neg",1)
    label_map.put("neu",2)
    label_map.put("pos",3)
    label_map
  }

  /**
    * 标题中不合格字符替换
    * @param title  替换前标题
    * @return  替换后标题
    */
  def replace(title:String):String={
//    println("字符替换！！")
    var title2=title.replace("/","每");
    title2=title2.replace("|","：");
    title2=title2.replace("：","：");
    title2=title2.replace("\"","“");
    title2=title2.replace("?","？");
    title2;
  }

}
