package io.github.kory33.guardedqueries.core.utils.extensions

import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.utils.datastructures.ImmutableStack
import java.util
import java.util.NoSuchElementException
import java.util.function.Function
import java.util.function.IntFunction
import java.util.stream.IntStream

object ListExtensions {
  def productMappedCollectionsToStacks[I, R](
    items: util.List[I], /* pure */ mapperToIterable: I => java.lang.Iterable[R]
  ): java.lang.Iterable[ImmutableStack[R]] = {
    val size = items.size
    val iterablesToProduct = items.stream.map(mapperToIterable(_)).toList
    val freshIteratorAt = (index: Int) => iterablesToProduct.get(index).iterator

    // we store iterator of the first iterable at the bottom of the stack
    val initialIteratorStack =
      ImmutableStack.fromIterable(IntStream.range(0, size).mapToObj(i =>
        freshIteratorAt(i)
      ).toList)

    () =>
      new util.Iterator[ImmutableStack[R]]() {
        private var currentIteratorStack = initialIteratorStack // the next stack to be returned
        // null if no more stack of elements can be produced
        private var currentItemStack: ImmutableStack[R] = null

        override def hasNext: Boolean = return currentItemStack != null

        private def advanceState(): Unit = {
          // invariant: during the loop, top `droppedIterators` have been exhausted
          //            after the loop, either the bottom iterator has been exhausted
          //            or all exhausted iterators have been replaced with fresh ones

          import scala.util.boundary
          boundary:
            for (droppedIterators <- 0 until size) {
              // currentIteratorStack is nonempty because it originally had `size` iterators
              val iteratorToAdvance = currentIteratorStack.unsafeHead
              if (iteratorToAdvance.hasNext) {
                currentItemStack =
                  currentItemStack.replaceHeadIfNonEmpty(iteratorToAdvance.next)
                // "restart" iterations of dropped top iterators from the beginning
                for (i <- 0 until droppedIterators) {
                  // we dropped top iterators, which are of last iterables in iterablesToProduct
                  val restartedIterator = freshIteratorAt.apply(size - droppedIterators + i)
                  currentIteratorStack = currentIteratorStack.push(restartedIterator)

                  // we checked in the class initializer that all fresh iterators have at least one element
                  // so as long as the mapperToIterable is pure, we can safely call next() here
                  val firstItemFromRestartedIterator = restartedIterator.next
                  currentItemStack = currentItemStack.push(firstItemFromRestartedIterator)
                }
                boundary.break()
              } else {
                currentIteratorStack = currentIteratorStack.dropHead
                currentItemStack = currentItemStack.dropHead
              }
            }

          // we have exhausted the bottom iterator, so we are done
          if (currentIteratorStack.isEmpty) currentItemStack = null
        }

        override def next: ImmutableStack[R] = {
          if (!hasNext) throw new NoSuchElementException
          val toReturn = currentItemStack
          advanceState()
          toReturn
        }
      }
  }

  def productMappedCollectionsToSets[I, R](
    items: util.List[I], /* pure */ mapperToIterable: I => java.lang.Iterable[R]
  ): java.lang.Iterable[ImmutableSet[R]] = () =>
    IteratorExtensions.mapInto(
      productMappedCollectionsToStacks(items, mapperToIterable(_)).iterator,
      s => ImmutableSet.copyOf(s)
    )
}
