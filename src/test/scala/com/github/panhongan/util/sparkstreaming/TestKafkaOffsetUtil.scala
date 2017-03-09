package com.github.panhongan.util.sparkstreaming

import org.apache.spark.streaming.kafka.OffsetRange

object TestKafkaOffsetUtil {
  
  def main(args : Array[String]) {
    val partition = 3
    
    var read_offsets = List[OffsetRange]()
    var write_offsets = Map[Integer, Long]()
    
    for (i <- 0 until partition) {
      read_offsets :+= OffsetRange.create("test", i, 30L, 21L)
      write_offsets += (new Integer(i) -> 30L)
    }
    
    val compare = KafkaOffsetUtil.compareConsumerAndProducerOffset(read_offsets, write_offsets)
    println(compare)
    
    read_offsets = KafkaOffsetUtil.reviseConsumerOffset(read_offsets, write_offsets)
     
    for (offset <- read_offsets) {
      println(offset.toString())
    }
  }
  
}