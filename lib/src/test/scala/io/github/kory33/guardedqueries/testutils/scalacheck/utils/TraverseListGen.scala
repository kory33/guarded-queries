package io.github.kory33.guardedqueries.testutils.scalacheck.utils

import org.scalacheck.Gen

object TraverseListGen {
  extension [T](list: List[T])
    def traverse[R](f: T => Gen[R]): Gen[List[R]] =
      if list.isEmpty then Gen.const(Nil)
      else for {
        head <- f(list.head)
        tail <- list.tail.traverse(f)
      } yield head :: tail
}
