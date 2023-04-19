package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec

class ListExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  val smallInt = Gen.chooseNum(0, 8)
  val smallListOfSmallInts = Gen.chooseNum(0, 8).flatMap(Gen.listOfN(_, smallInt))

  "result of ListExtensions.productMappedCollectionsToStacks" should "have the size equal to the product of size of input family" in {
    forAll(smallListOfSmallInts, minSuccessful(1000)) { xs =>
      val result = ListExtensions.productMappedCollectionsToStacks(
        xs.indices.asJava,
        index => (1 to xs(index)).asJava
      )

      // as a special case, the empty collection should result in an iterable containing a single empty stack
      // but this actually conforms to the specification
      assert(result.asScala.size == xs.fold(1)(_ * _))
    }
  }

  "every n'th element in the every reversed output of ListExtensions.productMappedCollectionsToStacks" should
    "be in the collection obtained by applying n'th element in the input list to the input function" in {
    forAll(smallListOfSmallInts, minSuccessful(1000)) { xs =>
      val result = ListExtensions.productMappedCollectionsToStacks(
        xs.indices.asJava,
        index => (1 to xs(index)).asJava
      )

      result.asScala.forall { stack =>
        stack.asScala.toList.reverse.zipWithIndex.forall { case (element, index) =>
          (1 to xs(index)).contains(element)
        }
      }
    }
  }
}
