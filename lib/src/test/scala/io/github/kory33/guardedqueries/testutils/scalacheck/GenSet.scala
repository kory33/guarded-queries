package io.github.kory33.guardedqueries.testutils.scalacheck

import org.scalacheck.Gen

object GenSet {
  def chooseSubset[T](set: Set[T]): Gen[Set[T]] = {
    val orderedSet = set.toVector
    val representationOfFullSet = (BigInt(1) << orderedSet.size) - 1

    Gen.chooseNum(BigInt(0), representationOfFullSet).map(representation =>
      orderedSet.zipWithIndex
        .filter { case (_, index) => representation.testBit(index) }
        .map(_._1)
        .toSet
    )
  }
}
