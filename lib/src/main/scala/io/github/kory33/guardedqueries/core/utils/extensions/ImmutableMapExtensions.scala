package io.github.kory33.guardedqueries.core.utils.extensions

import java.util
import java.util.Collections

object MapExtensions {
  def consumeAndCopy[K, V](entries: util.Iterator[_ <: util.Map.Entry[_ <: K, _ <: V]])
    : Map[K, V] = {
    val builder = Map.builder[K, V]
    entries.forEachRemaining((entry) => builder.put(entry.getKey, entry.getValue))
    builder.build
  }
}
