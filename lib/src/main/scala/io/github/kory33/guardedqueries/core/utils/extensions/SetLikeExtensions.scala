package io.github.kory33.guardedqueries.core.utils.extensions

import org.apache.commons.lang3.tuple.Pair
import java.math.BigInteger
import java.util
import java.util.Optional
import java.util.stream.IntStream
import java.util.stream.Stream
import scala.annotation.tailrec

object SetLikeExtensions {

  /**
   * Union of elements from two collections.
   */
  def union[T](collection1: util.Collection[_ <: T],
               collection2: util.Collection[_ <: T]
  ): Set[T] = Set.builder[T].addAll(collection1).addAll(collection2).build

  /**
   * Intersection of elements from two collections.
   */
  def intersection[T](collection1: util.Collection[_ <: T],
                      collection2: util.Collection[_ <: T]
  ): Set[T] = {
    val set2 = Set.copyOf(collection2)
    Set.copyOf(collection1.stream.filter(set2.contains).iterator)
  }

  /**
   * Check if two collections have any common elements.
   */
  def nontriviallyIntersects(collection1: util.Collection[_],
                             collection2: util.Collection[_]
  ): Boolean = {
    val set2 = Set.copyOf(collection2)
    collection1.stream.anyMatch(set2.contains)
  }

  /**
   * Check if two collections have no common elements.
   */
  def disjoint(collection1: util.Collection[_], collection2: util.Collection[_]): Boolean =
    !nontriviallyIntersects(collection1, collection2)

  /**
   * Set difference of elements from two collections.
   */
  def difference[T](collection1: util.Collection[_ <: T],
                    collection2: util.Collection[_ <: T]
  ): Set[T] = {
    val set2 = Set.copyOf(collection2)
    Set.copyOf(collection1.stream.filter((e) => !set2.contains(e)).iterator)
  }

  /**
   * Powerset of a set of elements from the given collection, lazily streamed.
   */
  def powerset[T](collection: util.Collection[_ <: T]): Stream[Set[T]] = {
    import scala.jdk.CollectionConverters._

    // deduplicated ArrayList of elements
    val arrayList = collection.asScala.toSet.toList
    val setSize = arrayList.size

    // every non-negative BigInteger less than this value represents a unique subset of the given collection
    val upperLimit = BigInteger.ONE.shiftLeft(setSize)

    StreamExtensions.unfold(
      BigInteger.ZERO,
      (currentIndex: BigInteger) => {
        // currentIndex < upperLimit
        if (currentIndex.compareTo(upperLimit) < 0) {
          val subset = Set.copyOf[T](IntStream.range(0, setSize).filter(
            currentIndex.testBit
          ).mapToObj(arrayList(_)).iterator)
          Optional.of(Pair.of(subset, currentIndex.add(BigInteger.ONE)))
        } else Optional.empty

      }
    )
  }

  /**
   * Saturate a collection of elements of type {@code T} by repeatedly applying a generator
   * function {@code generator} that generates a collection of elements of type {@code T} from
   * values of type {@code T}. <p> More precisely, the returned set is the smallest set {@code
   * S} such that <ol> <li>{@code S} contains all elements from the initial collection</li>
   * <li>for every element {@code t} of {@code S}, {@code generator.apply(t)} is contained in
   * {@code S}</li> </ol>
   */
  def generateFromElementsUntilFixpoint[T](
    initialCollection: util.Collection[_ <: T],
    generator: T => util.Collection[_ <: T]
  ): Set[T] = {
    val hashSet = new util.HashSet[T](initialCollection)
    var elementsAddedInPreviousIteration = Set.copyOf(hashSet)
    while (!elementsAddedInPreviousIteration.isEmpty) {
      val newlyGeneratedElements: Set[T] = {
        val builder = Set.builder[T]

        // assuming that the generator function is pure,
        // it is only meaningful to generate new elements
        // from elements that have been newly added to the set
        // in the previous iteration
        elementsAddedInPreviousIteration.forEach((newElementToConsider: T) => {
          import scala.jdk.CollectionConverters._
          for (generatedElement <- generator.apply(newElementToConsider).asScala) {
            // we only add elements that are not already in the set
            if (!hashSet.contains(generatedElement)) builder.add(generatedElement)
          }
        })
        builder.build
      }

      hashSet.addAll(newlyGeneratedElements)
      elementsAddedInPreviousIteration = newlyGeneratedElements
    }
    Set.copyOf(hashSet)
  }

  /**
   * Saturate a collection of elements of type {@code T} by repeatedly applying a generator
   * function {@code generator} that generates a collection of elements of type {@code T} from a
   * collection of values of type {@code T}. <p> More precisely, the returned set is the
   * smallest set {@code S} such that <ol> <li>{@code S} contains all elements from the initial
   * collection</li> <li>{@code generator.apply(S)} is contained in {@code S}</li> </ol>
   */
  def generateFromSetUntilFixpoint[T](
    initialCollection: util.Collection[_ <: T],
    generator: Set[T] => util.Collection[_ <: T]
  ): Set[T] = {
    val hashSet = new util.HashSet[T](initialCollection)

    @tailrec def recurse(): Set[T] = {
      val elementsGeneratedSoFar = Set.copyOf(hashSet)
      val elementsGeneratedInThisIteration =
        Set.copyOf[T](generator.apply(elementsGeneratedSoFar))

      if (hashSet.containsAll(elementsGeneratedInThisIteration)) {
        // we have reached the least fixpoint above initialCollection
        Set.copyOf(hashSet)
      } else {
        hashSet.addAll(elementsGeneratedInThisIteration)
        recurse()
      }
    }

    recurse()
  }
}
class SetLikeExtensions private {}
