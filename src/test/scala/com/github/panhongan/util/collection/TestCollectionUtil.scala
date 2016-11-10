package com.github.panhongan.util.collection

object TestCollectionUtil {
  
  def main(args : Array[String]) {
    val list = List("abc", "abc", "123", "345", "123")
    val iter = CollectionUtil.distinct(list.toIterator)
    while (iter.hasNext) {
      println(iter.next())
    }
  }
  
}