package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StringSetExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import StringSetExtensions.given

  ".freshPrefixStartingWith" should "return a string that is not a prefix of any element in the set and starts with the specified prefix" in {
    forAll(minSuccessful(1000)) { (xs: Set[String], prefix: String) =>
      val freshPrefix = xs.freshPrefixStartingWith(prefix)
      assert(freshPrefix.startsWith(prefix) && !xs.exists(_.startsWith(freshPrefix)))
    }
  }
}
