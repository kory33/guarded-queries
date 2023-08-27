package io.github.kory33.guardedqueries.core.utils

import java.util
import java.util.function.Function

final class CachingFunction[T, R](private val function: Function[T, R]) extends Function[T, R] {
  private var cache: util.HashMap[T, R] = new util.HashMap[T, R]

  override def apply(t: T): R =
    if (cache.containsKey(t)) cache.get(t)
    else {
      val result = function.apply(t)
      cache.put(t, result)
      result
    }
}
