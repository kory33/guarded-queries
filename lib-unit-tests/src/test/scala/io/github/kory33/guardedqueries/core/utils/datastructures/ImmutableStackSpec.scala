package io.github.kory33.guardedqueries.core.utils.datastructures

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import io.github.kory33.guardedqueries.core.utils.datastructures.ImmutableStack
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

private[this] val arbitraryImmutableStack = for {
  list <- arbitrary[List[Int]]
} yield ImmutableStack.fromIterable(list.asJava)

class ImmutableStackSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  ".iterator" should "traverse the construction list in a reverse order" in {
    forAll(minSuccessful(1000)) { (list: List[Int]) =>
      val stack = ImmutableStack.fromIterable(list.asJava)
      assert(stack.iterator.asScala.toList == list.reverse)
    }
  }

  ".reverse" should "reverse the stack" in {
    forAll(arbitraryImmutableStack, minSuccessful(1000)) { stack =>
      assert(stack.reverse.iterator.asScala.toList == stack.iterator.asScala.toList.reverse)
    }
  }

  ".dropHead" should "reduce the size by 1, unless the input is empty" in {
    forAll(arbitraryImmutableStack, minSuccessful(1000)) { stack =>
      assert(stack.dropHead.asScala.size == Math.max(0, stack.asScala.size - 1))
    }
  }
}
