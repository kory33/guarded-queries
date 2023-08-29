package io.github.kory33.guardedqueries.core.utils.extensions

import com.google.common.collect.ImmutableBiMap
import java.util
import java.util.stream.Stream

object MapExtensions {

  /**
   * Computes a map that maps each value in {@code values} to its preimage in {@code map}.
   */
  def preimages[K, V](map: util.Map[K, V], values: util.Collection[V]): Map[V, Set[K]] = {
    val valueSet = Set.copyOf(values)
    MapExtensions.consumeAndCopy(valueSet.stream.map((value: V) => {
      val preimageIterator = map.entrySet.stream.filter((entry: util.Map.Entry[K, V]) =>
        entry.getValue == value
      ).map(_.getKey).iterator
      util.Map.entry(value, Set.copyOf(preimageIterator))
    }).iterator)
  }

  def composeWithFunction[K, V1, V2](
    map: util.Map[K, V1],
    function: V1 => V2
  ): Map[K, V2] =
    MapExtensions.consumeAndCopy(map.entrySet.stream.map((entry: util.Map.Entry[K, V1]) =>
      util.Map.entry(entry.getKey, function.apply(entry.getValue))
    ).iterator)

  def restrictToKeys[K, V](map: util.Map[K, V], keys: util.Collection[K]): Map[K, V] =
    MapExtensions.consumeAndCopy(Set.copyOf(keys).stream.flatMap((key: K) =>
      if (map.containsKey(key)) Stream.of(util.Map.entry(key, map.get(key)))
      else Stream.empty
    ).iterator)

  def restrictToKeys[K, V](map: ImmutableBiMap[K, V],
                           keys: util.Collection[K]
  ): ImmutableBiMap[K, V] = {
    // this call to ImmutableBiMap.copyOf never throws since
    // a restriction of an injective map is again injective
    ImmutableBiMap.copyOf(restrictToKeys(map.asInstanceOf[util.Map[K, V]], keys))
  }
}
