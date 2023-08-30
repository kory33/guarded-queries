package io.github.kory33.guardedqueries.core.utils.extensions

import java.util
import java.util.NoSuchElementException
import scala.jdk.CollectionConverters._
import scala.util.boundary

object ListExtensions {
  given Extensions: {} with
    extension [I](list: List[I])
      def productMappedIterablesToLists[R](
        /* pure */ mapperToIterable: I => Iterable[R]
      ): Iterator[List[R]] = {
        val iterablesToProduct = list.map(mapperToIterable(_)).toList
        val freshIteratorAt = (index: Int) => iterablesToProduct(index).iterator

        // we store iterator of the first iterable at the bottom of the stack
        val initialIteratorStack = (0 until list.size).map(freshIteratorAt).toList

        new Iterator[List[R]] {
          private var currentIteratorStack: List[Iterator[R]] = initialIteratorStack

          // the next stack to be returned
          // None if no more stack of elements can be produced
          private var currentItemStack: Option[List[R]] = Some(Nil)

          // initialize currentItemStack
          {
            if (currentIteratorStack.exists(!_.hasNext)) {
              // if any iterator is empty at the beginning, we cannot produce any stack of elements
              currentItemStack = None
            } else {
              currentItemStack = Some(currentIteratorStack.map(_.next))
            }
          }

          override def hasNext: Boolean = currentItemStack.isDefined

          private def advanceState(): Unit = {
            // invariant: during the loop, top `droppedIterators` have been exhausted
            //            after the loop, either the bottom iterator has been exhausted
            //            or all exhausted iterators have been replaced with fresh ones
            boundary:
              for (droppedIterators <- 0 until list.size) {
                // currentIteratorStack is nonempty because it originally had `list.size` iterators
                val iteratorToAdvance = currentIteratorStack.head

                if (!iteratorToAdvance.hasNext) {
                  // keep dropping exhausted iterators
                  currentIteratorStack = currentIteratorStack.tail
                  currentItemStack = Some(currentItemStack.get.tail)
                } else {
                  currentItemStack = Some(iteratorToAdvance.next :: currentItemStack.get.tail)

                  // "restart" iterations of dropped top-`droppedIterators` iterators from the beginning
                  val restartedIterators =
                    (0 until droppedIterators).map(freshIteratorAt).toList

                  // we checked in the class initializer that all fresh iterators have at least one element
                  // so as long as the mapperToIterable is pure, we can safely call `next` here
                  currentItemStack =
                    Some(restartedIterators.map(_.next) ++ currentItemStack.get)
                  currentIteratorStack = restartedIterators ++ currentIteratorStack

                  boundary.break()
                }
              }

            // we have exhausted the bottom iterator, so we are done
            if (currentIteratorStack.isEmpty) currentItemStack = None
          }

          override def next: List[R] = {
            if (!hasNext) throw new NoSuchElementException
            val toReturn = currentItemStack.get
            advanceState()
            toReturn
          }
        }
      }
}
