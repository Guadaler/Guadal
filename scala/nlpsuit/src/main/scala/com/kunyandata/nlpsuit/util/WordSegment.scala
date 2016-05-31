package com.kunyandata.nlpsuit.util

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.Date
import com.kunyandata.nlpsuit.net.Packet

import scala.collection.mutable.ListBuffer

/**
  * Created by yangshuai on 5/9/16.
  * 分词类
  * 通过网络调用分词服务
  */
object WordSegment {

  val PACKET_HEAD_LENGTH = 26
  val OPERATE_CODE_SEGMENT = 0x7D1
  val OPERATE_CODE_END = 0x7D3
  val TOKEN = "12345678901234567890123456789012"

  /**
    * 分词
    *
    * @param content 需要分词的文本内容
    * @param host 分词服务 host
    * @param port 分词服务端口
    * @return 分词结果集合 List[(词, 此类型)]
    */
  def split(content: String, host: String, port: Int): List[(String, Int)] = {

    val socket = new Socket(host, port)
    val input = new DataInputStream(socket.getInputStream)
    val output = new DataOutputStream(socket.getOutputStream)

    val packets = getPackets(content)

    packets.foreach(bytes => {
      output.write(bytes)
      output.flush()
    })

    var list = List[(String, Int)]()
    val headBytes = new Array[Byte](PACKET_HEAD_LENGTH)
    var loopEnd = false
    var packetNumber = 0

    while (!loopEnd) {

      var len = input.readFully(headBytes)
      val operationCode = Packet.copyShortFromByte(headBytes, 6)
      packetNumber += 1

      if (operationCode == OPERATE_CODE_END) {

        loopEnd = true

      } else {

        val length = Packet.copyShortFromByte(headBytes, 0)
        val bodyBytes = new Array[Byte](length - PACKET_HEAD_LENGTH)
        len = input.readFully(bodyBytes)

        list ++= getWords(bodyBytes)

      }

    }

    input.close()
    output.close()
    socket.close()

    list
  }

  /**
    * 从包中提取出分词结果
    *
    * @param bytes 包对应的 byte 数组
    * @return 分词结果Array[(词, 词类型)]
    */
  def getWords(bytes: Array[Byte]): Array[(String, Int)] = {

    val size = bytes.length / 36
    val arr = new Array[(String, Int)](size)

    for (i <- 0 until size) {

      val wordBytes = new Array[Byte](32)
      System.arraycopy(bytes, 36 * i, wordBytes, 0, 32)
      val word = new String(wordBytes, "UTF-8").trim
      val wordType = Packet.copyIntFromByte(bytes, 32 + 36 * i)
      arr(i) = (word, wordType)

    }

    arr
  }

  def getPackets(content: String): ListBuffer[Array[Byte]] = {

    val list = ListBuffer[Array[Byte]]()

    if (content.getBytes.size < 9000) {
      list += getPacket(content)
    } else {
      TextUtil.splitArticle(content).foreach(x => {
        list += getPacket(x)
      })
    }

    list += getEndPacket

    list
  }

  def getPacket(content: String): Array[Byte] = {

    val packetLength = PACKET_HEAD_LENGTH + 8 + 32 + content.getBytes("UTF-8").length
    val bytes = new Array[Byte](packetLength)

    Packet.copyByteFromShort(packetLength, bytes, 0)
    bytes(2) = 0
    bytes(3) = 0
    Packet.copyByteFromShort(0, bytes, 4)
    Packet.copyByteFromShort(OPERATE_CODE_SEGMENT, bytes, 6)
    Packet.copyByteFromShort(8 + 32 + content.getBytes.length, bytes, 8)
    Packet.copyByteFromInt((new Date().getTime / 1000).toInt, bytes, 10)
    Packet.copyByteFromLong(0, bytes, 14)
    Packet.copyByteFromInt(0, bytes, 22)

    Packet.copyByteFromLong(0, bytes, 26)
    Packet.copyByteFromString(TOKEN, bytes, 34, 32)
    Packet.copyByteFromUTF(content, bytes, 34 + 32, content.getBytes.length)

    bytes
  }

  /**
    * 获取标志结束的包
    *
    */
  def getEndPacket: Array[Byte] = {

    val bytes = new Array[Byte](26)

    Packet.copyByteFromShort(26, bytes, 0)
    bytes(2) = 0
    bytes(3) = 0
    Packet.copyByteFromShort(0, bytes, 4)
    Packet.copyByteFromShort(OPERATE_CODE_END, bytes, 6)
    Packet.copyByteFromShort(0, bytes, 8)
    Packet.copyByteFromInt((new Date().getTime / 1000).toInt, bytes, 10)
    Packet.copyByteFromLong(0, bytes, 14)
    Packet.copyByteFromInt(0, bytes, 22)

    bytes
  }

}
