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

  /**
   * Returns a map that is the union of the given maps. If the same key occurs in multiple maps,
   * the value found in the last map is used.
   */
  @SafeVarargs def union[K, V](maps: util.Map[_ <: K, _ <: V]*): ImmutableMap[K, V] = {
    // because ImmutableMap.Builder#putAll does not override existing entries
    // (and instead throws IllegalArgumentException), we need to keep track
    // the keys we have added so far
    // we reverse the input array so that we can "throw away" key-conflicting entries
    // that appear first in the input array
    import scala.jdk.CollectionConverters._
    val inputMaps = maps.reverse
    val keysWitnessed = new util.HashSet[K]
    val builder = ImmutableMap.builder[K, V]
    for (map <- inputMaps) {
      for (entry <- map.entrySet.asScala) {
        if (keysWitnessed.add(entry.getKey)) builder.put(entry.getKey, entry.getValue)
      }
    }
    builder.build
  }
}
