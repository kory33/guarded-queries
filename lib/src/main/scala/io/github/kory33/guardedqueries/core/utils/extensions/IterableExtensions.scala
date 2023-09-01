package io.github.kory33.guardedqueries.core.utils.extensions

object IterableExtensions {
  given Extension: AnyRef with
    extension [T](i: Iterable[T])
      def intersects[U >: T](set: Set[U]): Boolean = i.exists(set.contains)

      def disjointFrom[U >: T](set: Set[U]): Boolean = !i.intersects(set)

      def associate[V](extractValue: T => V): Map[T, V] = i.map { x =>
        (x, extractValue(x))
      }.toMap
}
