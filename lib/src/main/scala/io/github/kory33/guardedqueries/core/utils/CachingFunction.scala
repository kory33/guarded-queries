package io.github.kory33.guardedqueries.core.utils

import scala.collection.mutable

final class CachingFunction[T, R](private val function: T => R) extends (T => R) {
  private var cache: mutable.HashMap[T, R] = mutable.HashMap.empty

  override def apply(t: T): R =
    if (cache.contains(t)) cache(t)
    else {
      val result = function(t)
      cache.put(t, result)
      result
    }
}

object CachingFunction {
  def apply[T, R](function: T => R): CachingFunction[T, R] = new CachingFunction[T, R](function)
}
