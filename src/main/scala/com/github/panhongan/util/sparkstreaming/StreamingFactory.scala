package com.github.panhongan.util.sparkstreaming

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
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
    
    var start_offsets = Map[TopicAndPartition, Long]()
    
    // read last offset from zk
    val last_read_offsets = KafkaOffsetUtil.readOffset(zk_list, 
        offset_zk_path, 
        kafka_param.get("group.id").get, 
        topic, 
        partition_num)
    
    var total_read_offset = 0L
    for (offset <- last_read_offsets) {
        logger.info("start from, topic = " + offset.topic +
          ", partition = " + offset.partition +
          ", start_offset = " + offset.fromOffset + 
          ", end_offset = " + offset.untilOffset)
        
          total_read_offset += offset.untilOffset
    }

    // get lastest write offset
    if (reuse_last_data_when_largest) {
      val partition_lastest_write_offset = KafkaOffsetUtil.getLatestWriteOffset(kafka_param.get("metadata.broker.list").get, topic)
      if(partition_lastest_write_offset.isEmpty) {
        logger.warn("failed to get partition write offset, topic = {}", topic)
        System.exit(1)
      }
    
      var total_latest_write_offset = 0L
      partition_lastest_write_offset.foreach(x => {
        logger.info("topic = {}, partition = {}, lastest_write_offset = {}", 
            topic, String.valueOf(x._1), String.valueOf(x._2))
            
         total_latest_write_offset += x._2
      })
    
      if (total_read_offset == total_latest_write_offset) {
        for (offset <- last_read_offsets) {
          val write_offset = partition_lastest_write_offset.get(offset.partition).get
          start_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.fromOffset)
        }
      } else {
        for (offset <- last_read_offsets) {
          val write_offset = partition_lastest_write_offset.get(offset.partition).get
          start_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.untilOffset)
        }
      }
    } else {
      for (offset <- last_read_offsets) {
        start_offsets += (TopicAndPartition(offset.topic, offset.partition) -> offset.untilOffset)
      }
    }
    
    // create dstream
    var kafka_stream : InputDStream[(String, String)] = null
    
    if (start_offsets.isEmpty) { // create dstream by default
      kafka_stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streaming_context, kafka_param, Set(topic))
    } else {
      val msg_handler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
      kafka_stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streaming_context, kafka_param, start_offsets, msg_handler)
    }
    
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