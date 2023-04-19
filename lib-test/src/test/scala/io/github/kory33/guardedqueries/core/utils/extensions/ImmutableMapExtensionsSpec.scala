package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ImmutableMapExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  ".consumeAndCopy" should "be identity" in {
    forAll(minSuccessful(1000)) { (map: Map[String, Int]) =>
      assert {
        ImmutableMapExtensions.consumeAndCopy(map.asJava.entrySet().iterator()).asScala.toMap == map
      }
    }
  }

  ".union" should "be equivalent to .fold(empty)(_ ++ _)" in {
    forAll(minSuccessful(1000)) { (xs: List[Map[Int, Int]]) =>
      assert {
        ImmutableMapExtensions.union(xs.map(_.asJava).toArray*).asScala.toMap == xs.foldLeft(Map.empty[Int, Int])(_ ++ _)
      }
    }
  }
}
