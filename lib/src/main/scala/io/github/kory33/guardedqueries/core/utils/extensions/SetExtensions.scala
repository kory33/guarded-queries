package io.github.kory33.guardedqueries.core.utils.extensions

import scala.annotation.tailrec
import scala.collection.mutable

object SetExtensions {
  given Extension: AnyRef with
    extension [T](set: Set[T])
      /**
       * Powerset of a set of elements from the given collection, lazily iterated.
       */
      // TODO: refactor this; is there a more appropriate data structure to return here?
      def powerset: Iterable[Set[T]] = {
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
       * Saturate a collection of elements of type `T` by repeatedly applying a generator
       * function `generator` that generates a collection of elements of type `T` from values of
       * type `T`.
       *
       * More precisely, the returned set is the smallest set `S` such that <ol> <li>`S`
       * contains all elements from the initial collection</li> <li>for every element `t` of
       * `S`, `generator.apply(t)` is contained in `S`</li> </ol>
       */
      // TODO: refactor this; can we make the implementation more succinct?
      def generateFromElementsUntilFixpoint(generator: T => Set[T]): Set[T] = {
        val hashSet = mutable.HashSet.from(set)
        var elementsAddedInPreviousIteration = hashSet.toSet

        while (elementsAddedInPreviousIteration.nonEmpty) {
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
       * Saturate a collection of elements of type `T` by repeatedly applying a generator
       * function `generator` that generates a collection of elements of type `T` from a
       * collection of values of type `T`.
       *
       * More precisely, the returned set is the smallest set `S` such that <ol> <li>`S`
       * contains all elements from the initial collection</li> <li>`generator.apply(S)` is
       * contained in `S`</li> </ol>
       */
      def generateFromSetUntilFixpoint(generator: Set[T] => Set[T]): Set[T] = {
        @tailrec def recurse(elementsGeneratedSoFar: Set[T]): Set[T] = {
          val newlyGenerated = generator(elementsGeneratedSoFar)
          if (newlyGenerated.subsetOf(elementsGeneratedSoFar)) {
            // we have reached the least fixpoint above set
            elementsGeneratedSoFar
          } else {
            recurse(elementsGeneratedSoFar ++ newlyGenerated)
          }
        }

        recurse(set)
      }

      def widen[U >: T]: Set[U] = set.map(t => t)
}
