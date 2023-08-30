package io.github.kory33.guardedqueries.core.utils.extensions

import scala.annotation.tailrec
import scala.collection.mutable

object SetLikeExtensions {

  /**
   * Powerset of a set of elements from the given collection, lazily streamed.
   */
  def powerset[T](set: Set[T]): Iterable[Set[T]] = {
    val orderedSet = set.toList

    // every non-negative BigInteger less than this value represents a unique subset of the given collection
    val upperLimit = BigInt(1) << set.size

    Iterable.unfold(BigInt(0))((currentIndex: BigInt) => {
      if (currentIndex < upperLimit) {
        val subset = (0 until set.size).filter(currentIndex.testBit).map(orderedSet(_))
        Some((subset.toSet, currentIndex + 1))
      } else {
        None
      }
    })
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
    initialCollection: Set[T],
    generator: T => Set[T]
  ): Set[T] = {
    val hashSet = mutable.HashSet.from(initialCollection)
    var elementsAddedInPreviousIteration = hashSet.toSet

    while (!elementsAddedInPreviousIteration.isEmpty) {
      val newlyGeneratedElements: Set[T] =
        // assuming that the generator function is pure,
        // it is only meaningful to generate new elements
        // from elements that have been newly added to the set
        // in the previous iteration
        for {
          newElementToConsider <- elementsAddedInPreviousIteration
          generatedElement <- generator.apply(newElementToConsider)
          if !hashSet.contains(generatedElement)
        } yield generatedElement

      hashSet ++= newlyGeneratedElements
      elementsAddedInPreviousIteration = newlyGeneratedElements
    }

    hashSet.toSet
  }

  /**
   * Saturate a collection of elements of type {@code T} by repeatedly applying a generator
   * function {@code generator} that generates a collection of elements of type {@code T} from a
   * collection of values of type {@code T}. <p> More precisely, the returned set is the
   * smallest set {@code S} such that <ol> <li>{@code S} contains all elements from the initial
   * collection</li> <li>{@code generator.apply(S)} is contained in {@code S}</li> </ol>
   */
  def generateFromSetUntilFixpoint[T](
    initialCollection: Set[T],
    generator: Set[T] => Set[T]
  ): Set[T] = {
    @tailrec def recurse(elementsGeneratedSoFar: Set[T]): Set[T] = {
      val newlyGenerated = generator(elementsGeneratedSoFar)
      if (newlyGenerated.subsetOf(elementsGeneratedSoFar)) {
        // we have reached the least fixpoint above initialCollection
        elementsGeneratedSoFar
      } else {
        recurse(elementsGeneratedSoFar ++ newlyGenerated)
      }
    }

    recurse(initialCollection)
  }
}
