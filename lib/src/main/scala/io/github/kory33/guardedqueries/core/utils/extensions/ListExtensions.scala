package io.github.kory33.guardedqueries.core.utils.extensions

import java.util
import java.util.NoSuchElementException
import java.util.stream.IntStream
import scala.jdk.CollectionConverters._
import scala.util.boundary

object ListExtensions {
  private def productMappedCollectionsToLists[I, R](
    items: List[I], /* pure */ mapperToIterable: I => Iterable[R]
  ): Iterator[List[R]] = {
    val iterablesToProduct = items.map(mapperToIterable(_)).toList
    val freshIteratorAt = (index: Int) => iterablesToProduct(index).iterator

    // we store iterator of the first iterable at the bottom of the stack
    val initialIteratorStack = (0 until items.size).map(freshIteratorAt).toList

    new Iterator[List[R]] {
      private var currentIteratorStack: List[Iterator[R]] = initialIteratorStack

      // the next stack to be returned
      // None if no more stack of elements can be produced
      private var currentItemStack: Option[List[R]] = Some(Nil)

      // initialize currentItemStack
      {
        // we put "the element from the iterator at the bottom of initialIteratorStack"
        // at the bottom of currentItemStack
        boundary:
          for (iterator <- initialIteratorStack.reverse) {
            if (iterator.hasNext) {
              currentItemStack = Some(iterator.next() :: currentItemStack.get)
            } else {
              // if any iterator is empty at the beginning, we cannot produce any stack of elements
              currentItemStack = None
              boundary.break()
            }
          }
      }

      override def hasNext: Boolean = currentItemStack.isDefined

      private def advanceState(): Unit = {
        // invariant: during the loop, top `droppedIterators` have been exhausted
        //            after the loop, either the bottom iterator has been exhausted
        //            or all exhausted iterators have been replaced with fresh ones
        boundary:
          for (droppedIterators <- 0 until items.size) {
            // currentIteratorStack is nonempty because it originally had `size` iterators
            val iteratorToAdvance = currentIteratorStack.head

            if (iteratorToAdvance.hasNext) {
              currentItemStack = Some(iteratorToAdvance.next :: currentItemStack.get.tail)

              // "restart" iterations of dropped top iterators from the beginning
              for (i <- 0 until droppedIterators) {
                // we dropped top iterators, which are of last iterables in iterablesToProduct
                val restartedIterator = freshIteratorAt.apply(items.size - droppedIterators + i)
                currentIteratorStack = restartedIterator :: currentIteratorStack

                // we checked in the class initializer that all fresh iterators have at least one element
                // so as long as the mapperToIterable is pure, we can safely call next() here
                val firstItemFromRestartedIterator = restartedIterator.next
                currentItemStack = Some(firstItemFromRestartedIterator :: currentItemStack.get)
              }

              boundary.break()
            } else {
              currentIteratorStack = currentIteratorStack.tail
              currentItemStack = Some(currentItemStack.get.tail)
            }
          }

        // we have exhausted the bottom iterator, so we are done
        if (currentIteratorStack.isEmpty) currentItemStack = null
      }

      override def next: List[R] = {
        if (!hasNext) throw new NoSuchElementException
        val toReturn = currentItemStack.get
        advanceState()
        toReturn
      }
    }
  }

  def productMappedCollectionsToSets[I, R](
    items: Set[I], /* pure */ mapperToIterable: I => Iterable[R]
  ): Iterator[Set[R]] =
    productMappedCollectionsToLists(items.toList, mapperToIterable(_)).map(_.toSet)
}
