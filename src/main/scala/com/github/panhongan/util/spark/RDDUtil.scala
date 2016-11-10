package com.github.panhongan.util.spark

import org.apache.spark.rdd.RDD
import com.github.panhongan.util.collection.CollectionUtil

object RDDUtil {
  
  def distinct(rdd : RDD[String]) : RDD[String] = {
    rdd.mapPartitions(CollectionUtil.distinct _).distinct()
  }
  
}