package com.github.panhongan.util.sparkstreaming

import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.zookeeper.ZooKeeper
import org.apache.log4j.Logger

import collection.JavaConversions

import com.github.panhongan.util.zookeeper.ZKUtil
import com.github.panhongan.util.kafka.KafkaUtil

import scala.util.control.Breaks

object KafkaOffsetUtil {

  private val logger = Logger.getLogger(KafkaOffsetUtil.getClass)

  val INVALID_OFFSET = -1

  def writeOffset(zk_list: String,
                  base_path: String,
                  group: String,
                  offset: OffsetRange): Boolean = {
    var zk: ZooKeeper = null
    var ret: Boolean = false

    try {
      zk = ZKUtil.connectZK(zk_list, 30000, null)
      if (zk != null) {
        var offset_path = base_path + "/" + group + "/" + offset.topic + "_" + offset.partition
        val value = offset.fromOffset + "_" + offset.untilOffset

        if (zk.exists(offset_path, false) == null) {
          ret = ZKUtil.createNodeRecursively(zk, offset_path, true)
          if (!ret) {
            ret
          }
        }

        zk.setData(offset_path, value.getBytes(), -1)
        ret = true
      }
    } catch {
      case e: Exception => {
        logger.warn(e.getMessage)
        ret = false
      }
    } finally {
      ZKUtil.closeZK(zk)
    }

    ret
  }

  def readOffset(zk_list: String,
                 base_path: String,
                 group: String,
                 topic: String,
                 partition_num: Int): List[OffsetRange] = {
    var offset = List[OffsetRange]()
    var zk: ZooKeeper = null

    try {
      zk = ZKUtil.connectZK(zk_list, 30000, null)

      var partition = 0
      while (partition < partition_num) {
        val offset_path = base_path + "/" + group + "/" + topic + "_" + partition

        if (zk.exists(offset_path, false) != null) {
          var value = new String(zk.getData(offset_path, false, null))
          var arr = value.split("_")
          if (arr != null && arr.length == 2) {
            offset :+= OffsetRange.create(topic, partition, arr(0).toLong, arr(1).toLong)
          }
        }

        partition += 1
      }
    } catch {
      case e: Exception => logger.warn(e.getMessage)
    } finally {
      ZKUtil.closeZK(zk)
    }

    offset
  }

  def getLatestWriteOffset(broker_list: String, topic: String): Map[Integer, Long] = {
    var ret_map = Map[Integer, Long]()

    try {
      val map: java.util.Map[Integer, java.lang.Long] = KafkaUtil.getLastestWriteOffset(broker_list, topic)
      val arr = map.keySet().toArray()
      for (i <- 0 until arr.length) {
        ret_map += (arr(i).asInstanceOf[Integer] -> map.get(arr(i)).asInstanceOf[scala.Long])
      }
    } catch {
      case ex: Exception => logger.warn(ex.getMessage, ex)
    }

    ret_map
  }

  def compareConsumerAndProducerOffset(read_offsets : List[OffsetRange], write_offsets: Map[Integer, Long]) : Integer = {
    var ret = 0
    var is_less = false
    var is_equal = false
    var is_greater = false

    val loop = new Breaks()
    loop.breakable(
      for (offset <- read_offsets) {
        val part = offset.partition
        val write_offset = write_offsets.get(part).get
        if (offset.untilOffset < write_offset) {
          is_less = true
          is_equal = false
        } else if (offset.untilOffset == write_offset) {
          if (!is_less) {
            is_equal = true
          }
        } else {
          is_greater = true
          loop.break
        }
      })
    
    if (is_less) {
      ret = -1
    } else if (is_equal) {
      ret = 0
    } else if (is_greater) {
      ret = 1
    }
    
    ret
  }
  
  def reviseConsumerOffset(read_offsets : List[OffsetRange], write_offsets: Map[Integer, Long]) : List[OffsetRange] = {
    var offset_ranges = List[OffsetRange]()
    
    for (offset <- read_offsets) {
      val part = offset.partition
      val write_offset = write_offsets.get(part).get
      
      if (offset.untilOffset <= write_offset) {
        offset_ranges :+= offset
      } else {
        offset_ranges :+= OffsetRange.create(offset.topic, offset.partition, write_offset, write_offset)
      }
    } // end for
    
    offset_ranges
  }

}
