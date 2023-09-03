package io.github.kory33.guardedqueries.core.utils.extensions

import scala.annotation.tailrec
import scala.collection.View
import IterableExtensions.given

object SetExtensions {
  given Extension: AnyRef with
    extension [T](set: Set[T])
      /**
       * Powerset of a set of elements from the given collection, lazily iterated and not
       * memoized.
       */
      def powerset: Iterable[Set[T]] = {
        val orderedSet = set.toList

        // every non-negative BigInteger less than this value represents a unique subset of the given collection
        val upperLimit = BigInt(1) << set.size

        // View.unfold is backed by a lazy iterator (UnfoldIterator) and not memoized
        View.unfold(BigInt(0))((currentIndex: BigInt) => {
          if (currentIndex < upperLimit) {
            val subset = (0 until set.size).filter(currentIndex.testBit).map(orderedSet(_))
            Some((subset.toSet, currentIndex + 1))
          } else {
            None
          }
        })
      }

      /**
       * Saturate this set by repeatedly applying a generator function `generator` that
       * generates a collection of elements of type `T` from values of type `T`.
       *
       * More precisely, the returned set is the smallest set `S` such that <ol> <li>`S`
       * contains all elements from the initial set</li> <li>for every element `t` of `S`,
       * `generator.apply(t)` is contained in `S`</li> </ol>
       */
      def generateFromElementsUntilFixpoint(generator: T => Set[T]): Set[T] = {
        @tailrec def recurse(
          elementsGeneratedSoFar: Set[T],
          elementsNewlyGeneratedInPreviousIteration: Set[T]
        ): Set[T] = {
          if (elementsNewlyGeneratedInPreviousIteration.isEmpty) {
            // we have reached the least fixpoint above the input set
            elementsGeneratedSoFar
          } else {
            val newlyGeneratedElements: Set[T] = for {
              // assuming that the generator function is pure,
              // it is only meaningful to generate new elements
              // from elements that have been newly added to the set
              // in the previous iteration
              newElementToConsider <- elementsNewlyGeneratedInPreviousIteration
              generatedElement <- generator.apply(newElementToConsider)
              if !elementsGeneratedSoFar.contains(generatedElement)
            } yield generatedElement

            recurse(
              elementsGeneratedSoFar ++ newlyGeneratedElements,
              newlyGeneratedElements
            )
          }
        }

        recurse(set, set)
      }

      /**
       * Saturate a collection of elements of type `T` by repeatedly applying a generator
       * function `generator` that generates a collection of elements of type `T` from a
       * collection of values of type `T`.
       *
       * More precisely, the returned set is the smallest set `S` such that <ol> <li>`S`
       * contains all elements from the initial collection</li> <li>`S union generator.apply(S)`
       * equals `S`</li> </ol>
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

      /**
       * Returns the [[Iterable]] that enumerates all sequences (with length `n`) of elements
       * from this set.
       */
      def naturalPowerTo(n: Int): Iterable[List[T]] = (0 until n).productAll(_ => set)

      /**
       * Union of all elements in this set, where each element is an iterable.
       */
      def unionAll[U](using ev: T <:< Iterable[U]): Set[U] = set.flatMap(ev(_))
}
