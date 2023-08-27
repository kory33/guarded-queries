package io.github.kory33.guardedqueries.core.utils

import com.google.common.collect.ImmutableBiMap
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.utils.extensions.ImmutableMapExtensions
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions
import org.apache.commons.lang3.tuple.Pair
import java.util
import java.util.Optional
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.Stream

object MappingStreams {
  def allTotalFunctionsBetween[K, V](domain: util.Collection[K],
                                     range: util.Collection[V]
  ): Stream[ImmutableMap[K, V]] = {
    val orderedDomain = ImmutableList.copyOf(ImmutableSet.copyOf(domain))
    val orderedRange = ImmutableList.copyOf(ImmutableSet.copyOf(range))
    val rangeSize = orderedRange.size
    /*
     * An internal state representing a mapping between domain and range.
     *
     * A value of this class is essentially an int array rangeElementIndices of length orderedDomain.size(),
     * representing a mapping that sends orderedDomain.get(i) to orderedRange.get(rangeElementIndices[i]).
     */
    class RangeIndexArray {
      final private[utils] val rangeElementIndices = new Array[Int](orderedDomain.size)
      // if we have reached the end of the stream
      private var reachedEnd = rangeSize == 0
      // if we have invoked toMap() after reaching the end of the stream
      private var _alreadyEmittedLastMap = reachedEnd && !orderedDomain.isEmpty

      /**
       * Increment index array. For example, if the array is [5, 4, 2] and rangeSize is 6, we
       * increment the array to [0, 5, 2] (increment the leftmost index that is not at rangeSize
       * \- 1, and clear all indices to the left). If all indices are at the maximum value, no
       * indices are modified and false is returned.
       */
      def increment(): Unit = {
        for (i <- 0 until rangeElementIndices.length) {
          if (rangeElementIndices(i) < rangeSize - 1) {
            rangeElementIndices(i) += 1
            for (j <- i - 1 to 0 by -1) { rangeElementIndices(j) = 0 }
            return
          }
        }
        reachedEnd = true
      }
      def alreadyEmittedLastMap: Boolean = _alreadyEmittedLastMap
      def currentToMap: ImmutableMap[K, V] = {
        if (reachedEnd) _alreadyEmittedLastMap = true
        ImmutableMapExtensions.consumeAndCopy(IntStream.range(
          0,
          rangeElementIndices.length
        ).mapToObj((i: Int) =>
          Pair.of(orderedDomain.get(i), orderedRange.get(rangeElementIndices(i)))
        ).iterator)
      }
    }
    StreamExtensions.unfoldMutable(
      new RangeIndexArray,
      (indexArray: RangeIndexArray) => {
        if (indexArray.alreadyEmittedLastMap) Optional.empty
        else {
          val output = indexArray.currentToMap
          indexArray.increment()
          Optional.of(output)
        }

      }
    )
  }
  def allPartialFunctionsBetween[K, V](domain: util.Collection[K],
                                       range: util.Collection[V]
  ): Stream[ImmutableMap[K, V]] = SetLikeExtensions.powerset(domain).flatMap(
    (domainSubset: ImmutableSet[K]) => allTotalFunctionsBetween(domainSubset, range)
  )
  def allInjectiveTotalFunctionsBetween[K, V](domain: util.Collection[K],
                                              range: util.Collection[V]
  ): Stream[ImmutableBiMap[K, V]] = {
    val orderedDomain = ImmutableList.copyOf(ImmutableSet.copyOf(domain))
    val orderedRange = ImmutableList.copyOf(ImmutableSet.copyOf(range))
    val rangeSize = orderedRange.size
    if (orderedDomain.size > rangeSize) return Stream.empty
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
      // [rangeSize-1, rangeSize-2, ..., rangeSize - orderedDomain.size()]
      private var _hasReachedEndAndIncrementAttempted = false

      /**
       * Increment index array. <p> We scan the index array from the end, and we try to
       * increment the entry (while maintaining injectivity) as early as possible. If we cannot
       * increment a particular entry, we drop it (conceptually, without actually resizing the
       * array) and try to increment the previous entry. After having incremented an entry, we
       * clear all entries to the right of it, and then we fill the cleared entries with the
       * smallest increasing sequence of integers that is not already used by previous entries.
       * <p> For example, if the array is [0,4,5] and rangeSize is 6, we first look at 5 and try
       * to increment it. Since 5 is not at the maximum value, we now consider the array to be
       * [0,4] and continue the process. Since 4 can be incremented, we increment it to 5. We
       * now have [0,5], so we fill the cleared entries with the smallest increasing sequence,
       * which is [1], so we end up with [0,5,1].
       */
      def increment(): Unit = {
        val availableIndices: util.HashSet[Integer] = null
        val usedIndices = util.Arrays.stream(rangeElementIndices).boxed.collect(
          Collectors.toCollection(util.HashSet(_))
        )
        availableIndices = IntStream.range(0, rangeSize).boxed.filter((i: Integer) =>
          !usedIndices.contains(i)
        ).collect(Collectors.toCollection(util.HashSet(_)))

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
        IntStream.range(0, rangeElementIndices.length).mapToObj((i: Int) =>
          Pair.of(orderedDomain.get(i), orderedRange.get(rangeElementIndices(i)))
        ).forEach(builder.put)
        builder.build
      }
    }
    StreamExtensions.unfoldMutable(
      new RangeIndexArray,
      (indexArray: RangeIndexArray) => {
        if (indexArray.hasReachedEndAndIncrementAttempted) Optional.empty
        else {
          val output = indexArray.toMap
          indexArray.increment()
          Optional.of(output)
        }

      }
    )
  }
}
class MappingStreams private {}
