package io.github.kory33.guardedqueries.core.utils.extensions

import scala.collection.AbstractView

object IterableExtensions {
  given Extension: AnyRef with
    extension [T](iterable: Iterable[T])

      /**
       * Returns true if the Iterable contains at least one element that belongs to the given
       * set.
       */
      def intersects[U >: T](set: Set[U]): Boolean = iterable.exists(set.contains)

      /**
       * Returns true if the Iterable contains no element that belongs to the given set.
       */
      def disjointFrom[U >: T](set: Set[U]): Boolean = !iterable.intersects(set)

      /**
       * Returns a map associating each element of the Iterable with the value extracted from it
       * by the given function.
       */
      def associate[V](extractValue: T => V): Map[T, V] = iterable.map { x =>
        (x, extractValue(x))
      }.toMap

      /**
       * Returns an [[Iterable]] that traverses all possible combinations of elements in the
       * list.
       *
       * Mathematically, one can think of this as the Cartesian product of sets indexed by the
       * list: `\prod_{i \in \mathrm{list}} \mathrm{mapperToIterable}(i)`.
       *
       * This is equivalent to the `traverse` function in Haskell
       * (https://hackage.haskell.org/package/base-4.18.0.0/docs/Data-Traversable.html#v:traverse)
       * or Cats
       * (https://github.com/typelevel/cats/blob/f496e2503f53ff09a7757f9a39920f0276297d27/core/src/main/scala/cats/Traverse.scala#L40-L55)
       */
      def productAll[R](mapperToIterable: T => Iterable[R]): Iterable[List[R]] = {
        def productRemaining(remaining: List[Iterable[R]]): Iterator[List[R]] =
          remaining match
            case Nil => Iterator(Nil)
            case head :: tail =>
              for {
                x <- head.iterator
                xs <- productRemaining(tail)
              } yield x :: xs

        val iterablesToProduct = iterable.toList.map(mapperToIterable(_))

        new AbstractView[List[R]] {
          override def iterator: Iterator[List[R]] = productRemaining(iterablesToProduct)
        }
      }
}
