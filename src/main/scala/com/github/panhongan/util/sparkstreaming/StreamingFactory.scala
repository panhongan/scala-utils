package com.github.panhongan.util.sparkstreaming

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges

import org.slf4j.LoggerFactory
import org.slf4j.Logger


object StreamingFactory {
  
  private val logger = LoggerFactory.getLogger(StreamingFactory.getClass)

  def createDirectStreamIgnoreOffset(topic_set : Set[String],
      kafka_param : Map[String, String],
      streaming_context : StreamingContext) : InputDStream[(String, String)] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streaming_context, kafka_param, topic_set)
  }
  
  def createDirectStreamByOffset(zk_list : String, 
      offset_zk_path : String,
      topic : String,
      partition_num : Int,
      kafka_param : Map[String, String],
      streaming_context : StreamingContext,
      reuse_last_data_when_largest : Boolean) : InputDStream[(String, String)] = {
    
    var start_read_offsets = Map[TopicAndPartition, Long]()
    
    // latest read offset from zk
    var latest_read_offsets = KafkaOffsetUtil.readOffset(zk_list, 
        offset_zk_path, 
        kafka_param.get("group.id").get, 
        topic,
        partition_num)
    if (latest_read_offsets.isEmpty) {
      for (i <- 0 until partition_num) {
        latest_read_offsets :+= OffsetRange.create(topic, i, 0L, 0L)
      }
    }
    
    for (offset <- latest_read_offsets) {
        logger.info("start from, topic = " + offset.topic +
          ", partition = " + offset.partition +
          ", start_offset = " + offset.fromOffset + 
          ", end_offset = " + offset.untilOffset)
    }
    
    // latest write offset
    val latest_write_offsets = KafkaOffsetUtil.getLatestWriteOffset(kafka_param.get("metadata.broker.list").get, topic)
    if(latest_write_offsets.isEmpty) {
      logger.warn("failed to get partition write offset, topic = {}", topic)
      System.exit(1)
    }
    
    val compare = KafkaOffsetUtil.compareConsumerAndProducerOffset(latest_read_offsets, latest_write_offsets)
    if (compare == 0) { // largest offset
      if (reuse_last_data_when_largest) {
        for (offset <- latest_read_offsets) {
          start_read_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.fromOffset)
        }
      } else {
        for (offset <- latest_read_offsets) {
          start_read_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.untilOffset)
        }
      } // end else
    } else if (compare < 0) { //
      for (offset <- latest_read_offsets) {
          start_read_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.untilOffset)
      }
    } else if (compare > 0) { // consumer offset > producer offset (invalid)
      val revised_latest_read_offsets = KafkaOffsetUtil.reviseConsumerOffset(latest_read_offsets, latest_write_offsets)
      for (offset <- revised_latest_read_offsets) {
          start_read_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.untilOffset)
      }
    }
    
    // create dstream
    val msg_handler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
    var kafka_stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        streaming_context, kafka_param, start_read_offsets, msg_handler)
    
    save_offset(kafka_stream, zk_list,
        offset_zk_path,
        kafka_param.get("group.id").get)
    
    kafka_stream
  }
  
  def save_offset(kafka_stream : InputDStream[(String, String)],
      zk_list : String,
      offset_zk_path : String,
      consumer_group : String) {
    kafka_stream.foreachRDD(rdd => {
        val offsetArr = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offset <- offsetArr) {
          val ret = KafkaOffsetUtil.writeOffset(zk_list, offset_zk_path, 
              consumer_group, offset)
          if (!ret) {
            logger.warn("write offset failed : group_id = " + consumer_group + ", offset = " + offset)
          } else {
            logger.info("write offset succeed : group_id = " + consumer_group + ", offset = " + offset)
          }
        }
    })
  }

}
