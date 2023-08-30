package io.github.kory33.guardedqueries.core.utils

import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap

import scala.collection.IterableOnce
import scala.util.boundary

import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given

object FunctionSpaces {
  def allTotalFunctionsBetween[K, V](domain: Set[K], range: Set[V]): IterableOnce[Map[K, V]] = {
    val orderedDomain = domain.toList
    val orderedRange = range.toList

    /*
     * An internal state representing a mapping between domain and range.
     *
     * A value of this class is essentially an int array rangeElementIndices of length orderedDomain.size(),
     * representing a mapping that sends orderedDomain.get(i) to orderedRange.get(rangeElementIndices[i]).
     */
    class RangeIndexArray {
      final private[utils] val rangeElementIndices = new Array[Int](orderedDomain.size)

      // if we have reached the end of the stream
      private var reachedEnd = range.isEmpty || domain.isEmpty

      // if we have invoked toMap() after reaching the end of the stream
      private var _alreadyEmittedLastMap = reachedEnd && domain.nonEmpty

      /**
       * Increment index array. For example, if the array is [5, 4, 2] and range.size is 6, we
       * increment the array to [0, 5, 2] (increment the leftmost index that is not at
       * range.size \- 1, and clear all indices to the left). If all indices are at the maximum
       * value, no indices are modified and false is returned.
       */
      private def increment(): Unit = boundary {
        for (i <- rangeElementIndices.indices) {
          if (rangeElementIndices(i) < range.size - 1) {
            rangeElementIndices(i) += 1
            for (j <- i - 1 to 0 by -1) { rangeElementIndices(j) = 0 }
            boundary.break()
          }
        }
        reachedEnd = true
      }

      def alreadyEmittedLastMap: Boolean = _alreadyEmittedLastMap

      private def currentToMap: Map[K, V] =
        rangeElementIndices.indices
          .map(i => (orderedDomain(i), orderedRange(rangeElementIndices(i))))
          .toMap

      def currentToMapAndIncrement: Map[K, V] = {
        val output = currentToMap
        increment()
        if (reachedEnd) _alreadyEmittedLastMap = true
        output
      }
    }

    Iterable.unfold(new RangeIndexArray)(indexArray => {
      // FIXME: this mutates RangeIndexArray, which is a bit weird
      if (indexArray.alreadyEmittedLastMap) {
        None
      } else {
        Some((indexArray.currentToMapAndIncrement, indexArray))
      }
    })
  }

  def allPartialFunctionsBetween[K, V](domain: Set[K], range: Set[V]): IterableOnce[Map[K, V]] =
    domain.powerset.flatMap(allTotalFunctionsBetween(_, range))

  def allInjectiveTotalFunctionsBetween[K, V](
    domain: Set[K],
    range: Set[V]
  ): IterableOnce[BijectiveMap[K, V]] = {
    val orderedDomain = domain.toList
    val orderedRange = range.toList
    if (orderedDomain.size > range.size) return Set.empty

    /*
     * An internal state representing an injective mapping between domain and range.
     *
     * A value of this class is essentially an int array rangeElementIndices of length orderedDomain.size(),
     * and produces injective mappings in the lexicographical order.
     */
    class RangeIndexArray {
      // [0,1,...,orderedDomain.size()-1] is the first injective mapping in the lexicographical order
      final private val rangeElementIndices: Array[Int] = orderedDomain.indices.toArray

      // boolean indicating whether we have called increment() after
      // reaching the maximum rangeElementIndices, which is
      // [range.size-1, range.size-2, ..., range.size - orderedDomain.size()]
      private var _hasReachedEndAndIncrementAttempted: Boolean = false

      /**
       * Increment index array.
       *
       * We scan the index array from the end, and we try to increment the entry (while
       * maintaining injectivity) as early as possible. If we cannot increment a particular
       * entry, we drop it (conceptually, without actually resizing the array) and try to
       * increment the previous entry. After having incremented an entry, we clear all entries
       * to the right of it, and then we fill the cleared entries with the smallest increasing
       * sequence of integers that is not already used by previous entries.
       *
       * For example, if the array is [0,4,5] and range.size is 6, we first look at 5 and try to
       * increment it. Since 5 is not at the maximum value, we now consider the array to be
       * [0,4] and continue the process. Since 4 can be incremented, we increment it to 5. We
       * now have [0,5], so we fill the cleared entries with the smallest increasing sequence,
       * which is [1], so we end up with [0,5,1].
       */
      def increment(): Unit = boundary {
        var availableIndices: Set[Int] = {
          val usedIndices = rangeElementIndices.toSet
          (0 until range.size).filter(!usedIndices.contains(_)).toSet
        }

        for (i <- rangeElementIndices.length - 1 to 0 by -1) {
          val oldEntry = rangeElementIndices(i)
          val incrementableTo = availableIndices.find(_ > oldEntry)

          if (incrementableTo.isDefined) {
            val newEntry = incrementableTo.get
            rangeElementIndices(i) = newEntry
            availableIndices = availableIndices + oldEntry - newEntry

            val sortedAvailableIndices = availableIndices.toArray.sorted
            for (j <- i + 1 until rangeElementIndices.length) {
              rangeElementIndices(j) = sortedAvailableIndices(j - i - 1)
            }
            boundary.break()
          } else {
            // we "drop" the entry and conceptually shorten the array
            availableIndices += oldEntry
          }
        }
        _hasReachedEndAndIncrementAttempted = true
      }
      def hasReachedEndAndIncrementAttempted: Boolean = _hasReachedEndAndIncrementAttempted

      def toMap: BijectiveMap[K, V] = BijectiveMap.tryFromInjectiveMap {
        rangeElementIndices.zipWithIndex
          .map((rangeIndex, domainIndex) =>
            (orderedDomain(domainIndex), orderedRange(rangeIndex))
          )
          .toMap
      }.get
    }

    Iterable.unfold(new RangeIndexArray)(indexArray => {
      if (indexArray.hasReachedEndAndIncrementAttempted) {
        None
      } else {
        val output = indexArray.toMap
        indexArray.increment()
        Some((output, indexArray))
      }
    })
  }
}
