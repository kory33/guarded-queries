package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.Gen.*
import org.scalacheck.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IterableExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import IterableExtensions.given

  val smallInt: Gen[Int] = Gen.chooseNum(0, 8)
  val smallListOfSmallInts: Gen[List[Int]] =
    Gen.chooseNum(0, 8).flatMap(Gen.listOfN(_, smallInt))

  given Shrink[List[Int]] = Shrink.shrinkAny

  "result of .productAll" should "have the size equal to the product of size of input family" in {
    forAll(smallListOfSmallInts, minSuccessful(1000)) { xs =>
      val result = xs.productAll(1 to _)

      // As a special case, the empty input list should result in an Iterable
      // containing a single empty List, but this actually conforms to the specification
      // since the empty product equals 1.
      assert(result.size == xs.product)
    }
  }

  "every n-th element in the every output of .productAll" should
    "be in the collection obtained by applying n-th element in the input list to the input function" in {
      forAll(smallListOfSmallInts, minSuccessful(1000)) { xs =>
        val result = xs.productAll(1 to _)

        assert {
          result.forall { list =>
            list.zipWithIndex.forall { (element, index) => (1 to xs(index)).contains(element) }
          }
        }
      }
    }
}
