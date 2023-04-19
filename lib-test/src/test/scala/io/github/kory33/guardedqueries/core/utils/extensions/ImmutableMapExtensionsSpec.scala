package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*

object ImmutableMapExtensionsSpec extends Properties("ImmutableMapExtensions") {
  import Prop.forAll

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(1000)

  property("consumeAndCopy should be identity") = forAll { (map: Map[String, Int]) =>
    ImmutableMapExtensions.consumeAndCopy(map.asJava.entrySet().iterator()).asScala.toMap == map
  }

  property("union should be equivalent to .fold(empty)(_ ++ _)") = forAll { (xs: List[Map[Int, Int]]) =>
    ImmutableMapExtensions.union(xs.map(_.asJava).toArray*).asScala.toMap == xs.foldLeft(Map.empty[Int, Int])(_ ++ _)
  }
}
