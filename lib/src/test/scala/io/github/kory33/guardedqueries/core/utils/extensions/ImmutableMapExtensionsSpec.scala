package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MapExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  ".consumeAndCopy" should "be identity" in {
    forAll(minSuccessful(1000)) { (map: Map[String, Int]) =>
      assert {
        MapExtensions.consumeAndCopy(
          map.asJava.entrySet().iterator()
        ).toMap == map
      }
    }
  }
}
