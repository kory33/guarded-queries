package io.github.kory33.guardedqueries.core.utils.extensions

import org.apache.commons.lang3.tuple.Pair
import java.util
import java.util.function.Function
import java.util.stream.LongStream
import java.util.stream.Stream
import java.util.stream.StreamSupport

object IteratorExtensions {

  /**
   * Zip two iterators together.
   */
  def zip[L, R](leftIterator: util.Iterator[L],
                rightIterator: util.Iterator[R]
  ): util.Iterator[Pair[L, R]] = new util.Iterator[Pair[L, R]]() {
    override def hasNext: Boolean = leftIterator.hasNext && rightIterator.hasNext

    override def next: Pair[L, R] = Pair.of(leftIterator.next, rightIterator.next)
  }

  /**
   * Zip the iterator together with an infinite Long stream starting from 0.
   */
  def zipWithIndex[T](iterator: util.Iterator[T]): util.Iterator[Pair[T, java.lang.Long]] =
    zip(iterator, LongStream.range(0, Long.MaxValue).iterator)

  /**
   * Convert an iterator to a stream.
   */
  def intoStream[T](iterator: util.Iterator[T]): Stream[T] = {
    val iterable: java.lang.Iterable[T] = () => iterator
    StreamSupport.stream(iterable.spliterator, false)
  }

  def mapInto[R, T](iterator: util.Iterator[_ <: T],
                    mapper: Function[_ >: T, _ <: R]
  ): util.Iterator[R] = new util.Iterator[R]() {
    override def hasNext: Boolean = iterator.hasNext

    override def next: R = mapper.apply(iterator.next)
  }
}
