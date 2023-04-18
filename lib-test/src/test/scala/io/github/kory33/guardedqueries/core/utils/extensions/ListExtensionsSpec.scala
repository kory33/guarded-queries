package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*

object ListExtensionsSpec extends Properties("ListExtensions") {
  import Prop.forAll

  val smallInt = Gen.chooseNum(0, 8)
  val smallListOfSmallInts = Gen.chooseNum(0, 8).flatMap(Gen.listOfN(_, smallInt))

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(1000)

  property("size of result of productMappedCollectionsToStacks should be the product of size of input family") =
    forAll(smallListOfSmallInts) { xs =>
      val result = ListExtensions.productMappedCollectionsToStacks(
        xs.indices.asJava,
        index => (1 to xs(index)).asJava
      )

      // as a special case, the empty collection should result in an iterable containing a single empty stack
      // but this actually conforms to the specification
      result.asScala.size == xs.fold(1)(_ * _)
    }

  property(
    "every n'th element in every reversed stack from productMappedCollectionsToStacks " + 
    "should be in the collection obtained by applying n'th element in the input list to the input function"
  ) = forAll(smallListOfSmallInts) { xs =>
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
