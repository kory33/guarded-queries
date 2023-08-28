package io.github.kory33.guardedqueries.core.utils.extensions

import com.google.common.collect.ImmutableMap
import java.util
import java.util.Collections

object ImmutableMapExtensions {
  def consumeAndCopy[K, V](entries: util.Iterator[_ <: util.Map.Entry[_ <: K, _ <: V]])
    : ImmutableMap[K, V] = {
    val builder = ImmutableMap.builder[K, V]
    entries.forEachRemaining((entry) => builder.put(entry.getKey, entry.getValue))
    builder.build
  }
}
