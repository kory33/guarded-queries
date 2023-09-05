package io.github.kory33.guardedqueries.testutils.scalacheck.utils

import org.scalacheck.Shrink

object ShrinkList {
  def shrinkEachIn[T](list: List[T])(using Shrink[T]): LazyList[List[T]] = list match {
    case Nil => LazyList.empty
    case head :: tail =>
      val headShrunkLists = LazyList.from(Shrink.shrink(head).map(_ :: tail))
      val tailShrunkLists = shrinkEachIn(tail).map(head :: _)
      headShrunkLists ++ tailShrunkLists
  }
}
