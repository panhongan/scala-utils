package com.github.panhongan.util.collection

import scala.collection.mutable
import scala.collection.immutable

object SetUtil {
  
  def toImmutableSet(set : mutable.Set[String]) : immutable.Set[String] = {
    immutable.Set[String]() ++ set
  }
  
  def toMutableSet(set : immutable.Set[String]) : mutable.Set[String] = {
    mutable.Set[String]() ++ set
  }
  
}