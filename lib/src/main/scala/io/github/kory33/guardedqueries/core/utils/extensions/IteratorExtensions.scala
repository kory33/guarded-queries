package io.github.kory33.guardedqueries.core.utils.extensions

import org.apache.commons.lang3.tuple.Pair
import java.util
import java.util.stream.LongStream
import java.util.stream.Stream
import java.util.stream.StreamSupport

object IteratorExtensions {

  /**
   * Convert an iterator to a stream.
   */
  def intoStream[T](iterator: util.Iterator[T]): Stream[T] = {
    val iterable: java.lang.Iterable[T] = () => iterator
    StreamSupport.stream(iterable.spliterator, false)
  }

  def mapInto[R, T](iterator: util.Iterator[_ <: T], mapper: T => R): util.Iterator[R] =
    new util.Iterator[R]() {
      override def hasNext: Boolean = iterator.hasNext

      override def next: R = mapper.apply(iterator.next)
    }
}
