package com.github.panhongan.util.collection

import scala.collection.mutable
import scala.collection.immutable

object TestSetUtil {
  
  def main(args : Array[String]) {
    val s1 = mutable.Set[String]()
    s1 += "a"
    println(s1.toString())
    
    val s2 = SetUtil.toImmutableSet(s1);
    println(s2.toString())
    //s2 += "b";
    
    val s3 = immutable.Set[String]("11");
    println(s3.toString())
    
    val s4 = SetUtil.toMutableSet(s3)
    println(s4.toString())
    s4 += "22"
    println(s4.toString())
  }
  
}
