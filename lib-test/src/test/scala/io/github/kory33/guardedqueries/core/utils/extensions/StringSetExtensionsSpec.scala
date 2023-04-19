package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*

object StringSetExtensionsSpec extends Properties("StringSetExtensions") {
  import Prop.forAll

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(5000)

  property("isPrefixOfSome should be equivalent to .exists(_.startsWith(prefix))") = forAll { (xs: List[String], prefix: String) =>
    StringSetExtensions.isPrefixOfSome(xs.asJava, prefix) == xs.exists(_.startsWith(prefix))
  }

  property("freshPrefix should return a string that is not a prefix of any element in the set and starts with the specified prefix") =
    forAll { (xs: Set[String], prefix: String) =>
      val freshPrefix = StringSetExtensions.freshPrefix(xs.asJava, prefix)
      freshPrefix.startsWith(prefix) && !xs.exists(_.startsWith(freshPrefix))
    }
}
