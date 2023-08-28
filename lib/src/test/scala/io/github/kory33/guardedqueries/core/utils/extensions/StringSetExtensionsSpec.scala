package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StringSetExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  ".isPrefixOfSome" should "be equivalent to .exists(_.startsWith(prefix))" in {
    forAll(minSuccessful(1000)) { (xs: Set[String], prefix: String) =>
      assert(
        StringSetExtensions.isPrefixOfSome(xs, prefix) == xs.exists(_.startsWith(prefix))
      )
    }
  }

  ".freshPrefix" should "return a string that is not a prefix of any element in the set and starts with the specified prefix" in {
    forAll(minSuccessful(1000)) { (xs: Set[String], prefix: String) =>
      val freshPrefix = StringSetExtensions.freshPrefix(xs, prefix)
      assert(freshPrefix.startsWith(prefix) && !xs.exists(_.startsWith(freshPrefix)))
    }
  }
}
