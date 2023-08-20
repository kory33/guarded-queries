package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec

class IteratorExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  ".zip" should "be equivalent to Scala's zip" in {
    forAll(minSuccessful(1000)) { (xs: List[Int], ys: List[Int]) =>
      assert {
        IteratorExtensions
          .zip(xs.asJava.iterator(), ys.asJava.iterator())
          .asScala.map(p => (p.getKey(), p.getValue()))
          .toList == xs.zip(ys)
      }
    }
  }

  ".zipWithIndex" should "be equivalent to Scala's zipWithIndex" in {
    forAll(minSuccessful(1000)) { (xs: List[Int]) =>
      assert {
        IteratorExtensions
          .zipWithIndex(xs.asJava.iterator())
          .asScala.map(p => (p.getKey(), p.getValue()))
          .toList == xs.zipWithIndex
      }
    }
  }

  ".mapInto and then toList" should "be the same as map" in {
    forAll(minSuccessful(1000)) { (xs: List[Int]) =>
      assert {
        IteratorExtensions
          .mapInto(xs.asJava.iterator(), (x: Int) => x * 2)
          .asScala
          .toList == xs.map(_ * 2)
      }
    }
  }
}
