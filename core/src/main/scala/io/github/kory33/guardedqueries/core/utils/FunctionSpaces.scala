package io.github.kory33.guardedqueries.core.utils

import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given

import scala.collection.View
import scala.collection.immutable.BitSet

object FunctionSpaces {

  /**
   * Returns an iterable of all total functions from domain to range.
   */
  def allFunctionsBetween[K, V](domain: Set[K], range: Set[V]): Iterable[Map[K, V]] = {
    val orderedDomain = domain.toVector
    val orderedRange = range.toVector

    orderedDomain.indices
      .productAll(_ => orderedRange)
      .map((orderedRangeValues: List[V]) => orderedDomain.zip(orderedRangeValues).toMap)
  }

  /**
   * Returns an iterable of all partial functions from domain to range.
   */
  def allPartialFunctionsBetween[K, V](domain: Set[K], range: Set[V]): Iterable[Map[K, V]] =
    domain.powerset.flatMap(allFunctionsBetween(_, range))

  /**
   * Returns an iterable of all injective total functions from domain to range.
   */
  def allInjectionsBetween[K, V](
    domain: Set[K],
    range: Set[V]
  ): Iterable[BijectiveMap[K, V]] = {
    val orderedDomain = domain.toList
    val orderedRange = range.toList
    if (orderedDomain.size > range.size) return Set.empty

    type RangeIndex = Int

    def injectiveIndexMatches(
      remainingDomainSize: Int,
      remainingRangeIndices: Set[RangeIndex]
    ): Iterator[List[RangeIndex]] = {
      if (remainingDomainSize == 0) Iterator(Nil)
      else
        for {
          nextRangeIndexToUse <- remainingRangeIndices.iterator
          remainingMatching <- injectiveIndexMatches(
            remainingDomainSize - 1,
            remainingRangeIndices - nextRangeIndexToUse
          )
        } yield nextRangeIndexToUse :: remainingMatching
    }

    def allInjectionsIterator() =
      injectiveIndexMatches(orderedDomain.size, BitSet.fromSpecific(orderedRange.indices))
        .map { (rangeIndices: List[RangeIndex]) =>
          val map = orderedDomain.zip(rangeIndices.map(orderedRange)).toMap

          BijectiveMap.tryFromInjectiveMap(map).get
        }

    View.fromIteratorProvider(() => allInjectionsIterator())
  }
}
