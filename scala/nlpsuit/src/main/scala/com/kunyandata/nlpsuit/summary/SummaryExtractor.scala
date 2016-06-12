package com.kunyandata.nlpsuit.summary

import java.io.{DataOutputStream, DataInputStream}
import java.net.Socket
import java.util.Date

import com.kunyandata.nlpsuit.net.Packet
import com.kunyandata.nlpsuit.util.TextUtil

import scala.collection.mutable.ListBuffer

/**
  * Created by yang on 5/31/16.
  */
object SummaryExtractor {

  val PACKET_HEAD_LENGTH = 26
  val ARTICLE_DIGEST_UNIT = 0xBB9
  val ARTICLE_DIGEST_END = 0xBBA
  val ARTICLE_RESULT_DIGEST = 0xBBB

  def extractSummary(content: String, host: String, port: Int): String = {

    val socket = new Socket(host, port)
    socket.setSoTimeout(10000)
    val input = new DataInputStream(socket.getInputStream)
    val output = new DataOutputStream(socket.getOutputStream)

    val packets = getPackets(content)

    packets.foreach(bytes => {
      output.write(bytes)
      output.flush()
    })

    val headBytes = new Array[Byte](PACKET_HEAD_LENGTH)
    var summary = ""

    try {

      input.readFully(headBytes)
      val operationCode = Packet.copyShortFromByte(headBytes, 6)

      if (operationCode == ARTICLE_RESULT_DIGEST) {

        val length = Packet.copyShortFromByte(headBytes, 0)
        val bodyBytes = new Array[Byte](length - PACKET_HEAD_LENGTH)
        input.readFully(bodyBytes)

        summary = Packet.copyUTFFromByte(bodyBytes, 4, bodyBytes.length - 4)
      }

    } catch {
      case e: Exception =>
        throw e
    }

    input.close()
    output.close()
    socket.close()

    summary
  }

  def getPackets(content: String): ListBuffer[Array[Byte]] = {

    val timeStamp = (new Date().getTime % 100000).toInt

    val list = ListBuffer[Array[Byte]]()

    if (content.getBytes.size < 9000) {
      list += getPacket(timeStamp, content)
    } else {
      TextUtil.splitArticle(content).foreach(x => {
        list += getPacket(timeStamp, x)
      })
    }

    list += getEndPacket(timeStamp)

    list
  }

  def getPacket(id: Int, content: String): Array[Byte] = {

    val packetLength = PACKET_HEAD_LENGTH + 4 + content.getBytes("UTF-8").length
    val bytes = new Array[Byte](packetLength)

    Packet.copyByteFromShort(packetLength, bytes, 0)
    bytes(2) = 0
    bytes(3) = 0
    Packet.copyByteFromShort(0, bytes, 4)
    Packet.copyByteFromShort(ARTICLE_DIGEST_UNIT, bytes, 6)
    Packet.copyByteFromShort(4 + content.getBytes.length, bytes, 8)
    Packet.copyByteFromInt((new Date().getTime / 1000).toInt, bytes, 10)
    Packet.copyByteFromLong(0, bytes, 14)
    Packet.copyByteFromInt(0, bytes, 22)

    Packet.copyByteFromInt(id, bytes, 26)
    Packet.copyByteFromUTF(content, bytes, 30, content.getBytes.length)

    bytes
  }

  /**
    * 获取标志结束的包
    *
    */
  def getEndPacket(id: Int): Array[Byte] = {

    val bytes = new Array[Byte](30)

    Packet.copyByteFromShort(30, bytes, 0)
    bytes(2) = 0
    bytes(3) = 0
    Packet.copyByteFromShort(0, bytes, 4)
    Packet.copyByteFromShort(ARTICLE_DIGEST_END, bytes, 6)
    Packet.copyByteFromShort(0, bytes, 8)
    Packet.copyByteFromInt((new Date().getTime / 1000).toInt, bytes, 10)
    Packet.copyByteFromLong(0, bytes, 14)
    Packet.copyByteFromInt(0, bytes, 22)

    Packet.copyByteFromInt(id, bytes, 26)

    bytes
  }

}
