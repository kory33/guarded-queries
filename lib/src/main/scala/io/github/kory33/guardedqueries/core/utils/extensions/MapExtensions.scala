package io.github.kory33.guardedqueries.core.utils.extensions

import scala.jdk.CollectionConverters.*
import com.google.common.collect.ImmutableBiMap
import java.util
import java.util.stream.Stream

object MapExtensions {

  /**
   * Computes a map that maps each value in {@code values} to its preimage in {@code map}.
   */
  def preimages[K, V](map: Map[K, V], values: Set[V]): Map[V, Set[K]] =
    map.groupMap(_._2)(_._1)
      .view
      .mapValues(_.toSet)
      .filterKeys(values.contains)
      .toMap

  def restrictToKeys[K, V](map: Map[K, V], keys: Set[K]): Map[K, V] =
    map.view.filterKeys(keys.contains).toMap

  def restrictToKeys[K, V](map: ImmutableBiMap[K, V], keys: Set[K]): ImmutableBiMap[K, V] =
    // this call to ImmutableBiMap.copyOf never throws since
    // a restriction of an injective map is again injective
    ImmutableBiMap.copyOf(restrictToKeys(map.asScala.toMap, keys).asJava)
}
