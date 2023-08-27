package io.github.kory33.guardedqueries.core.utils.extensions

import com.google.common.collect.ImmutableMap
import java.util._

object ImmutableMapExtensions {
  def consumeAndCopy[K, V](entries: util.Iterator[_ <: util.Map.Entry[_ <: K, _ <: V]])
    : ImmutableMap[K, V] = {
    val builder = ImmutableMap.builder[K, V]
    entries.forEachRemaining((entry: _$1) => builder.put(entry.getKey, entry.getValue))
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
    val inputMaps = new util.ArrayList[util.Map[_ <: K, _ <: V]](util.Arrays.asList(maps))
    Collections.reverse(inputMaps)
    val keysWitnessed = new util.HashSet[K]
    val builder = ImmutableMap.builder[K, V]
    import scala.collection.JavaConversions._
    for (map <- inputMaps) {
      import scala.collection.JavaConversions._
      for (entry <- map.entrySet) {
        if (keysWitnessed.add(entry.getKey)) builder.put(entry.getKey, entry.getValue)
      }
    }
    builder.build
  }
}
