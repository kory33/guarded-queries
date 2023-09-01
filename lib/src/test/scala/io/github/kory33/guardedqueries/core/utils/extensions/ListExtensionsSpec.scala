package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.Gen.*
import org.scalacheck.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ListExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import ListExtensions.given

  val smallInt: Gen[Int] = Gen.chooseNum(0, 8)
  val smallListOfSmallInts: Gen[List[Int]] =
    Gen.chooseNum(0, 8).flatMap(Gen.listOfN(_, smallInt))

  given Shrink[List[Int]] = Shrink.shrinkAny

  "result of .productMappedIterablesToLists" should "have the size equal to the product of size of input family" in {
    forAll(smallListOfSmallInts, minSuccessful(1000)) { xs =>
      val result =
        xs.indices.toList.productMappedIterablesToLists(index => (1 to xs(index)).toSet)

      // as a special case, the empty collection should result in an Iterable containing a single empty stack
      // but this actually conforms to the specification
      assert(result.size == xs.product)
    }
  }

  "every n-th element in the every output of .productMappedIterablesToLists" should
    "be in the collection obtained by applying n-th element in the input list to the input function" in {
      forAll(smallListOfSmallInts, minSuccessful(1000)) { xs =>
        val result =
          xs.indices.toList.productMappedIterablesToLists(index => (1 to xs(index)).toSet)

        assert {
          result.forall { stack =>
            stack.zipWithIndex.forall {
              case (element, index) =>
                (1 to xs(index)).contains(element)
            }
          }
        }
      }
    }
}
