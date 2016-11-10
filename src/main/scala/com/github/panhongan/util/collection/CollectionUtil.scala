package com.github.panhongan.util.collection

import scala.collection.mutable.Set

object CollectionUtil {

  def distinct(iter: Iterator[String]): Iterator[String] = {
    val set = Set[String]()

    while (iter.hasNext) {
      set += iter.next()
    }

    set.toIterator
  }

}