package io.github.kory33.guardedqueries.core.utils.extensions

import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap

import java.util
import java.util.stream.Stream
import scala.jdk.CollectionConverters.*

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

  def restrictToKeys[K, V](map: BijectiveMap[K, V], keys: Set[K]): BijectiveMap[K, V] =
  // this call to .get never throws since a restriction of an injective map is again injective
    BijectiveMap.tryFromInjectiveMap(restrictToKeys(map, keys)).get
}
