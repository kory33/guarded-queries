package io.github.kory33.guardedqueries.core.utils.extensions

object IterableExtensions {
  given Extension: AnyRef with
    extension [T](i: Iterable[T])

      /**
       * Returns true if the Iterable contains at least one element that belongs to the given
       * set.
       */
      def intersects[U >: T](set: Set[U]): Boolean = i.exists(set.contains)

      /**
       * Returns true if the Iterable contains no element that belongs to the given set.
       */
      def disjointFrom[U >: T](set: Set[U]): Boolean = !i.intersects(set)

      /**
       * Returns a map associating each element of the Iterable with the value extracted from it
       * by the given function.
       */
      def associate[V](extractValue: T => V): Map[T, V] = i.map { x =>
        (x, extractValue(x))
      }.toMap
}
