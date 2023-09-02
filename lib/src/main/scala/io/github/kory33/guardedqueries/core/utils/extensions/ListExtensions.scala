package io.github.kory33.guardedqueries.core.utils.extensions

import scala.collection.{AbstractView, View}

object ListExtensions {
  given Extensions: AnyRef with
    extension [I](list: List[I])
      /**
       * Returns a [[View]] that traverses all possible combinations of elements in the list.
       *
       * Mathematically, one can think of this as the Cartesian product of sets indexed by the
       * list: `\prod_{i \in \mathrm{list}} \mathrm{mapperToIterable}(i)`.
       *
       * This is conceptually equivalent to the `traverse` function in Haskell
       * (https://hackage.haskell.org/package/base-4.18.0.0/docs/Data-Traversable.html#v:traverse)
       * or Cats
       * (https://github.com/typelevel/cats/blob/f496e2503f53ff09a7757f9a39920f0276297d27/core/src/main/scala/cats/Traverse.scala#L40-L55)
       */
      def productAll[R](
        mapperToIterable: I => /* immutable */ Iterable[R]
      ): /* immutable */ View[Vector[R]] = {
        val iterablesToProduct = list.map(mapperToIterable(_))

        enum ComponentState:
          case ProducedValue(value: R, iterator: Iterator[R])
          case Fresh(iterator: Iterator[R])

        object ComponentState:
          def freshStateAt(index: Int): ComponentState =
            ComponentState.Fresh(iterablesToProduct(index).iterator)

          given Extensions: AnyRef with
            extension (state: ComponentState)
              /**
               * Advance the iterator in the state and return the new state. The old state is
               * invalidated.
               */
              def advance(): ComponentState =
                val it = state match
                  case ComponentState.Fresh(it)            => it
                  case ComponentState.ProducedValue(_, it) => it
                ComponentState.ProducedValue(it.next(), it)

        new AbstractView[Vector[R]] {
          override def iterator: Iterator[Vector[R]] = new Iterator[Vector[R]] {
            // invariant:
            //  - states.length == list.size
            //  - 0 <= leftmostFreshIndex <= states.length
            //  - states(i) is fresh if and only if i <= leftmostFreshIndex
            private val states: Array[ComponentState] =
              Array.tabulate(list.size)(ComponentState.freshStateAt)
            private var leftmostFreshIndex = 0
            private var producedAnyElement: Boolean = false

            /**
             * Refresh iterators if necessary so that all iterators have next elements.
             *
             * Returns true if and only if iterators are ready to produce the next combination.
             * More precisely, this method returns false if and only if either
             *   - all iterators are exhausted, in which case we have produced all elements, or
             *   - some iterator is refreshed and is empty, in which case the Cartesian product
             *     is empty and we must terminate the [[Iterator]].
             *
             * If the return value is true, then all states are appropriately refreshed so that
             * iterators in [(leftmostFreshIndex - 1) ... states.length) are ready to be
             * advanced to produce the next combination.
             */
            private def prepareAllIteratorsToProduceValues(): Boolean = {
              if (leftmostFreshIndex == states.length) {
                // All states are ProducedValue, so either
                //  - we have just produced a combination, or
                //  - the input collection is empty.
                // We must now prepare iterators so that they can be advanced, and in doing so,
                // reset the states from the rightmost ones if necessary.

                // loop invariant:
                //  - 0 <= leftmostFreshIndex <= states.length
                //  - states(i) is fresh if and only if i <= leftmostFreshIndex
                while (leftmostFreshIndex > 0) {
                  // this cast is safe because states(leftmostFreshIndex - 1) is not Fresh
                  val oldState =
                    states(leftmostFreshIndex - 1).asInstanceOf[ComponentState.ProducedValue]

                  if (oldState.iterator.hasNext) {
                    // Since there is a ProducedValue state, we are in the middle of iteration
                    // and producedAnyElement == true.
                    // So all iterators in states[leftmostFreshIndex ... states.length) are nonempty,
                    // and we can advance iterators in states[(leftmostFreshIndex - 1) ... states.length)
                    // to produce the next combination.
                    // (The non-emptiness of the iterators in [leftmostFreshIndex ... states.length) is guaranteed
                    //  by the immutability of the Iterable returned by mapperToIterable.)
                    return true
                  } else {
                    leftmostFreshIndex -= 1
                    states(leftmostFreshIndex) =
                      ComponentState.freshStateAt(leftmostFreshIndex)
                  }
                }

                // Now leftmostFreshIndex == 0 and all states are Fresh.
                // So we have produced all elements unless producedAnyElement is false,
                // in which case states.length == 0 and the empty list is due to be produced.
                !producedAnyElement
              } else {
                // As 0 <= leftmostFreshIndex <= states.length, now we have 0 <= leftmostFreshIndex < states.length.
                // If we are here, we either
                //  - have already called this method after the last call to .next(), or
                //  - we are at the beginning or the end of iteration, in which case leftmostFreshIndex == 0.

                if (leftmostFreshIndex == 0) {
                  // All states are Fresh. If producedAnyElement is true, then we have already produced all elements.
                  !producedAnyElement && {
                    // If we have not produced an element, we can produce one only if all iterators are nonempty.
                    states.forall(state =>
                      // this cast is safe because all states are Fresh
                      state.asInstanceOf[ComponentState.Fresh]
                        .iterator
                        .hasNext
                    )
                  }
                } else {
                  // The first iterator is not fresh but there are fresh iterators, so
                  // we are in the middle of iteration.
                  // By the immutability of the Iterable returned by mapperToIterable,
                  // we should be able to produce an element again
                  // by advancing iterators after leftmostFreshIndex.
                  true
                }
              }
            }

            override def hasNext: Boolean = prepareAllIteratorsToProduceValues()

            /**
             * Returns the next combination of elements in the list.
             *
             * Postcondition:
             *   - producedAnyElement == true
             *   - leftmostFreshIndex == states.length
             */
            override def next: Vector[R] = {
              if (!prepareAllIteratorsToProduceValues()) {
                throw new NoSuchElementException("next on empty iterator")
              }

              // we are committed to produce an element at this point
              producedAnyElement = true

              // advance iterators from leftmostFreshIndex - 1 to the right
              ((0 max (leftmostFreshIndex - 1)) until states.length).foreach { index =>
                // these calls to .advance() are safe because prepareAllIteratorsToProduceValues() returned true
                states(index) = states(index).advance()
              }

              // advancing iterators in [(leftmostFreshIndex - 1) ... states.length)
              // makes all states ProducedValue, so we reset leftmostFreshIndex to states.length
              leftmostFreshIndex = states.length

              // extract the combination from ProducedValue states
              Vector.tabulate(states.length) { index =>
                // this cast is safe because all fresh iterators have been advanced
                states(index).asInstanceOf[ComponentState.ProducedValue].value
              }
            }
          }
        }
      }
}
