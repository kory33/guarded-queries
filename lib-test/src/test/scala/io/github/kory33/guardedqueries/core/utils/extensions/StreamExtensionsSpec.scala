package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import java.util.Optional
import org.apache.commons.lang3.tuple.Pair

object StreamExtensionsSpec extends Properties("StreamExtensions") {
  import Prop.*

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(1000)

  property("zipWithIndex.toList.asScala should be equivalent to Scala's zipWithIndex") = forAll { (xs: List[Int]) =>
    StreamExtensions
      .zipWithIndex(xs.asJava.stream())
      .toList
      .asScala.toList
      .map(p => (p.getKey(), p.getValue())) == xs.zipWithIndex
  }

  property("associate should be equivalent to Scala's .map(x => (x, f(x))).toMap") = forAll { (xs: List[Int], f: Int => String) =>
    StreamExtensions
      .associate(xs.asJava.stream(), f(_))
      .toList.asScala.map(e => (e.getKey(), e.getValue()))
      .toMap == xs.map(x => (x, f(x))).toMap
  }

  property("intoIterableOnce.toList should be identity") = forAll { (xs: List[Int]) =>
    StreamExtensions
      .intoIterableOnce(xs.asJava.stream())
      .asScala.toList == xs
  }

  property("filterSubtype should be the same as Scala's .collect { case x: T => x }") = forAll { (xs: List[Number]) =>
    StreamExtensions
      .filterSubtype(xs.asJava.stream(), classOf[Integer])
      .toList.asScala.toList == xs.collect { case x: Integer => x }
  }

  property("unfold should be the same as Scala's unfold") = forAll { (initial: BigInt) =>
    initial != 0 ==> {
      StreamExtensions
        .unfold(initial, x => if (x.abs < 1000) Optional.of(Pair.of(x.toString(), x * 2)) else Optional.empty())
        .toList.asScala.toList == List.unfold(initial)(x => if (x.abs < 1000) Some((x.toString(), x * 2)) else None)
    }
  }
}
