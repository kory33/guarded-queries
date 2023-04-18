package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*

object IteratorExtensionsSpec extends Properties("IteratorExtensions") {
  import Prop.forAll

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(1000)

  property("zip should be equivalent to Scala's zip") = forAll { (xs: List[Int], ys: List[Int]) =>
    IteratorExtensions
      .zip(xs.asJava.iterator(), ys.asJava.iterator())
      .asScala.map(p => (p.getKey(), p.getValue()))
      .toList == xs.zip(ys)
  }

  property("zipWithIndex should be equivalent to Scala's zipWithIndex") = forAll { (xs: List[Int]) =>
    IteratorExtensions
      .zipWithIndex(xs.asJava.iterator())
      .asScala.map(p => (p.getKey(), p.getValue()))
      .toList == xs.zipWithIndex
  }

  property("mapInto and then toList should be the same as map") = forAll { (xs: List[Int]) =>
    IteratorExtensions
      .mapInto(xs.asJava.iterator(), (x: Int) => x * 2)
      .asScala
      .toList == xs.map(_ * 2)
  }
}
