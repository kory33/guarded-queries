package io.github.kory33.guardedqueries.core.utils.datastructures

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import io.github.kory33.guardedqueries.core.utils.datastructures.ImmutableStack

private[this] val arbitraryImmutableStack = for {
  list <- arbitrary[List[Int]]
} yield ImmutableStack.fromIterable(list.asJava)

object ImmutableStackSpec extends Properties("SimpleUnionFindTree") {

  import Prop.forAll

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(1000)

  property("iterator of ImmutableStack created from a list should traverse the given list in a reverse order") = forAll { (list: List[Int]) =>
    val stack = ImmutableStack.fromIterable(list.asJava)
    stack.iterator.asScala.toList == list.reverse
  }

  property("reverse should reverse the stack") = forAll(arbitraryImmutableStack) { stack =>
    stack.reverse.iterator.asScala.toList == stack.iterator.asScala.toList.reverse
  }

  property("dropHead should reduce the size by 1, unless the input is empty") = forAll(arbitraryImmutableStack) { stack =>
    stack.dropHead.asScala.size == Math.max(0, stack.asScala.size - 1)
  }
}
