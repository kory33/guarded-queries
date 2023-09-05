package io.github.kory33.guardedqueries.testutils.scalacheck.utils

import org.scalacheck.Shrink

object ShrinkSet {
  private def oneElementRemovedFrom[T](v: Vector[T]): LazyList[Vector[T]] =
    LazyList.from(v.indices).map(i => v.take(i) ++ v.drop(i + 1))

  private def strictSubVectors[T](v: Vector[T]): LazyList[Vector[T]] =
    if (v.isEmpty) {
      LazyList.empty
    } else {
      val oneElementRemoved = oneElementRemovedFrom(v)
      oneElementRemoved ++ oneElementRemoved.flatMap(subVector => strictSubVectors(subVector))
    }

  def intoSubsets[T]: Shrink[Set[T]] = Shrink.withLazyList { set =>
    strictSubVectors(set.toVector).map(_.toSet)
  }
}
