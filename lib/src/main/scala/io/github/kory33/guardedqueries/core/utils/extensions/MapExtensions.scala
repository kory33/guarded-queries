package io.github.kory33.guardedqueries.core.utils.extensions

object MapExtensions {
  given Extensions: AnyRef with
    extension [K, V](map: Map[K, V])
      /**
       * Computes a map that maps each value in {@code values} to its preimage in {@code map}.
       */
      def preimages(values: Set[V]): Map[V, Set[K]] =
        map.groupMap(_._2)(_._1)
          .view
          .mapValues(_.toSet)
          .filterKeys(values.contains)
          .toMap

      def restrictToKeys(keys: Set[K]): Map[K, V] =
        map.view.filterKeys(keys.contains).toMap
}
