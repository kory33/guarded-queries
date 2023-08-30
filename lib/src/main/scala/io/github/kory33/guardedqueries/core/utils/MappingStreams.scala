package io.github.kory33.guardedqueries.core.utils

import com.google.common.collect.ImmutableBiMap
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions
import org.apache.commons.lang3.tuple.Pair

import java.util
import java.util.Optional
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.Stream
import scala.collection.IterableOnce

object MappingStreams {
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
      private var _alreadyEmittedLastMap = reachedEnd && !domain.isEmpty

      /**
       * Increment index array. For example, if the array is [5, 4, 2] and range.size is 6, we
       * increment the array to [0, 5, 2] (increment the leftmost index that is not at
       * range.size \- 1, and clear all indices to the left). If all indices are at the maximum
       * value, no indices are modified and false is returned.
       */
      private def increment(): Unit = {
        for (i <- 0 until rangeElementIndices.length) {
          if (rangeElementIndices(i) < range.size - 1) {
            rangeElementIndices(i) += 1
            for (j <- i - 1 to 0 by -1) { rangeElementIndices(j) = 0 }
            return
          }
        }
        reachedEnd = true
      }

      def alreadyEmittedLastMap: Boolean = _alreadyEmittedLastMap

      private def currentToMap: Map[K, V] =
        (0 until rangeElementIndices.length)
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
    SetLikeExtensions
      .powerset(domain)
      .flatMap(allTotalFunctionsBetween(_, range).toSet)

  def allInjectiveTotalFunctionsBetween[K, V](
    domain: Set[K],
    range: Set[V]
  ): IterableOnce[ImmutableBiMap[K, V]] = {
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
      final private val rangeElementIndices = IntStream.range(0, orderedDomain.size).toArray

      // boolean indicating whether we have called increment() after
      // reaching the maximum rangeElementIndices, which is
      // [range.size-1, range.size-2, ..., range.size - orderedDomain.size()]
      private var _hasReachedEndAndIncrementAttempted = false

      /**
       * Increment index array. <p> We scan the index array from the end, and we try to
       * increment the entry (while maintaining injectivity) as early as possible. If we cannot
       * increment a particular entry, we drop it (conceptually, without actually resizing the
       * array) and try to increment the previous entry. After having incremented an entry, we
       * clear all entries to the right of it, and then we fill the cleared entries with the
       * smallest increasing sequence of integers that is not already used by previous entries.
       * <p> For example, if the array is [0,4,5] and range.size is 6, we first look at 5 and
       * try to increment it. Since 5 is not at the maximum value, we now consider the array to
       * be [0,4] and continue the process. Since 4 can be incremented, we increment it to 5. We
       * now have [0,5], so we fill the cleared entries with the smallest increasing sequence,
       * which is [1], so we end up with [0,5,1].
       */
      def increment(): Unit = {
        val availableIndices: util.HashSet[Integer] = {
          import scala.jdk.CollectionConverters._

          val usedIndices = rangeElementIndices.toSet
          val set = new util.HashSet[Integer]
          (0 until range.size).filter(!usedIndices.contains(_)).foreach(i =>
            set.add(new Integer(i))
          )
          set
        }

        for (i <- rangeElementIndices.length - 1 to 0 by -1) {
          val oldEntry = rangeElementIndices(i)
          val incrementableTo =
            availableIndices.stream.filter((index: Integer) => index > oldEntry).findFirst
          if (incrementableTo.isPresent) {
            val newEntry = incrementableTo.get
            rangeElementIndices(i) = newEntry
            availableIndices.add(oldEntry)
            availableIndices.remove(newEntry)
            val sortedAvailableIndices = new util.ArrayList[Integer](availableIndices)
            sortedAvailableIndices.sort((a, b) => a.compareTo(b))
            for (j <- i + 1 until rangeElementIndices.length) {
              rangeElementIndices(j) = sortedAvailableIndices.get(j - i - 1)
            }
            return
          } else {
            // we "drop" the entry and conceptually shorten the array
            availableIndices.add(oldEntry)
          }
        }
        _hasReachedEndAndIncrementAttempted = true
      }
      def hasReachedEndAndIncrementAttempted: Boolean = _hasReachedEndAndIncrementAttempted
      def toMap: ImmutableBiMap[K, V] = {
        val builder = ImmutableBiMap.builder[K, V]
        (0 until rangeElementIndices.length).map(i =>
          Pair.of(orderedDomain(i), orderedRange(rangeElementIndices(i)))
        ).foreach(builder.put)
        builder.build
      }
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
