package com.github.panhongan.util.mapreduce


object ReduceUtil {

  def reduceAddInt(a: Int, b: Int): Int = {
    a + b
  }

  def reduceAddLong(a: Long, b: Long): Long = {
    a + b
  }

  def reduceAddFloat(a: Float, b: Float): Float = {
    a + b
  }

  def reduceMergeSet[T](a: Set[T], b: Set[T]): Set[T] = {
    a ++ b
  }

  def reduceMergeList[T](a: List[T], b: List[T]): List[T] = {
    a ++ b
  }

  def reduceMergeTuple(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
    (a._1 + b._1, a._2 + b._2)
  }

  def reduceMergeTupleOfFloat(a: (Float, Float), b: (Float, Float)): (Float, Float) = {
    (a._1 + b._1, a._2 + b._2)
  }

  def reduceMaxLong(a: Long, b: Long): Long = {
    math.max(a, b)
  }

}