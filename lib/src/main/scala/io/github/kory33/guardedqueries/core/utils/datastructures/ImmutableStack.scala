package io.github.kory33.guardedqueries.core.utils.datastructures

import java.util
import java.util.Collections
import java.util.NoSuchElementException

// TODO: replace with Scala List
object ImmutableStack {
  case class Cons[T](head: T, tail: ImmutableStack[T]) extends ImmutableStack[T] {
    override def iterator: util.Iterator[T] = new util.Iterator[T]() {
      private var current: ImmutableStack[T] = Cons.this

      override def hasNext: Boolean = !current.isEmpty

      override def next: T = {
        if (!hasNext) throw new NoSuchElementException("Nil")
        val oldCons = current.asInstanceOf[Cons[T]]
        current = oldCons.tail
        oldCons.head
      }
    }

    override def toString: String =
      import scala.jdk.CollectionConverters._
      s"ImmutableStack(${this.iterator.asScala.mkString(", ")}))"
  }

  object Nil {
    // we will share this singleton instance
    private val INSTANCE = new ImmutableStack.Nil[AnyRef]
    def getInstance[T]: ImmutableStack.Nil[T] = {
      // noinspection unchecked
      INSTANCE.asInstanceOf[ImmutableStack.Nil[T]]
    }
  }

  class Nil[T] private extends ImmutableStack[T] {
    override def iterator: util.Iterator[T] = Collections.emptyIterator
    override def toString = "ImmutableStack()"
  }

  def fromIterable[T](iterable: java.lang.Iterable[T]): ImmutableStack[T] = {
    var current: ImmutableStack[T] = Nil.getInstance
    import scala.jdk.CollectionConverters._
    for (item <- iterable.asScala) { current = current.push(item) }
    current
  }

  @SafeVarargs def of[T](items: T*): ImmutableStack[T] =
    import scala.jdk.CollectionConverters._
    fromIterable(items.asJava)

  def empty[T]: ImmutableStack[T] = Nil.getInstance
}

trait ImmutableStack[T] extends java.lang.Iterable[T] {
  def push(item: T) = new ImmutableStack.Cons[T](item, this)

  def isEmpty: Boolean =
    // equivalent to "this instanceof Nil" because Nil only has one instance
    this eq ImmutableStack.empty

  def unsafeHead: T =
    if (this.isEmpty) throw new NoSuchElementException("Nil")
    else this.asInstanceOf[ImmutableStack.Cons[T]].head

  def dropHead: ImmutableStack[T] =
    if (this.isEmpty) ImmutableStack.empty
    else this.asInstanceOf[ImmutableStack.Cons[T]].tail

  def replaceHeadIfNonEmpty(item: T): ImmutableStack[T] =
    if (this.isEmpty) ImmutableStack.empty
    else this.dropHead.push(item)

  def reverse: ImmutableStack[T] = {
    // fromIterable puts the first element of the iterable at the bottom of the stack
    // and the iterator implementation of ImmutableStack traverses the stack from the top to the bottom
    ImmutableStack.fromIterable(this)
  }
}
