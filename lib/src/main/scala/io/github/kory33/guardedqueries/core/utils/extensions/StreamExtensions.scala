package io.github.kory33.guardedqueries.core.utils.extensions

import org.apache.commons.lang3.tuple.Pair
import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function
import java.util.stream.Stream

object StreamExtensions {

  /**
   * From a given stream of elements of type {@code T}, create a new stream that associates each
   * element with its mapped value.
   */
  def associate[T, R](stream: Stream[T],
                      mapper: Function[_ >: T, _ <: R]
  ): Stream[util.Map.Entry[T, R]] = stream.map((t: T) => util.Map.entry(t, mapper.apply(t)))

  def intoIterableOnce[T](stream: Stream[T]): java.lang.Iterable[T] = () => stream.iterator

  def filterSubtype[T1, T2 <: T1](stream: Stream[T1], clazz: Class[T2]): Stream[T2] =
    stream.filter(clazz.isInstance).map(clazz.cast)

  /**
   * Create a stream that yields elements of type {@code Output} by mutating a mutable state of
   * type {@code MutableState}. The stream terminates when the function {@code
   * mutateAndYieldNext} returns an empty optional. <p> The function {@code mutateAndYieldNext}
   * should be a stateless {@code Function} object.
   */

  def unfoldMutable[MutableState, Output](
    mutableState: MutableState,
    mutateAndYieldNext: Function[_ >: MutableState, Optional[Output]]
  ): Stream[Output] = {
    val iterator = new util.Iterator[Output]() {
      private var nextOutput: Optional[Output] = mutateAndYieldNext.apply(mutableState)
      private var nextState: MutableState = mutableState

      override def hasNext: Boolean = nextOutput.isPresent
      override def next: Output = {
        val output = nextOutput.get
        nextOutput = mutateAndYieldNext.apply(nextState)
        output
      }
    }

    IteratorExtensions.intoStream(iterator)
  }

  /**
   * Create a stream that yields elements of type by repeatedly applying a function {@code
   * yieldNext} to a state of type {@code State}. The function {@code yieldNext} should be a
   * stateless {@code Function} object.
   */
  def unfold[State, Output](
    initialState: State,
    yieldNext: Function[_ >: State, Optional[_ <: Pair[_ <: Output, _ <: State]]]
  ): Stream[Output] = {
    val stateCell = new AtomicReference[State](initialState)
    unfoldMutable(
      stateCell,
      (cell: AtomicReference[State]) => {
        val nextPair = yieldNext.apply(cell.get)
        if (nextPair.isPresent) {
          cell.set(nextPair.get.getRight)
          Optional.of(nextPair.get.getLeft)
        } else Optional.empty

      }
    )
  }
}
