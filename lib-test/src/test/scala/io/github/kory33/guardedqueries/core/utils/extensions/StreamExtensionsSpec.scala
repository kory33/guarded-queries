package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import java.util.Optional
import org.apache.commons.lang3.tuple.Pair
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StreamExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import org.scalacheck.Prop.propBoolean

  ".zipWithIndex.toList.asScala" should "be equivalent to Scala's zipWithIndex" in {
    forAll(minSuccessful(1000)) { (xs: List[Int]) =>
      assert {
        StreamExtensions
          .zipWithIndex(xs.asJava.stream())
          .toList
          .asScala.toList
          .map(p => (p.getKey(), p.getValue())) == xs.zipWithIndex
      }
    }
  }

  ".associate" should "be equivalent to Scala's .map(x => (x, f(x))).toMap" in {
    forAll(minSuccessful(1000)) { (xs: List[Int], f: Int => String) =>
      assert {
        StreamExtensions
          .associate(xs.asJava.stream(), f(_))
          .toList.asScala.map(e => (e.getKey(), e.getValue()))
          .toMap == xs.map(x => (x, f(x))).toMap
      }
    }
  }

  ".intoIterableOnce.toList" should "be identity" in {
    forAll(minSuccessful(1000)) { (xs: List[Int]) =>
      assert {
        StreamExtensions
          .intoIterableOnce(xs.asJava.stream())
          .asScala.toList == xs
      }
    }
  }

  ".filterSubtype" should "be the same as Scala's .collect { case x: T => x }" in {
    forAll(minSuccessful(1000)) { (xs: List[Number]) =>
      assert {
        StreamExtensions
          .filterSubtype(xs.asJava.stream(), classOf[Integer])
          .toList.asScala.toList == xs.collect { case x: Integer => x }
      }
    }
  }

  ".unfold" should "be the same as Scala's unfold" in {
    forAll(minSuccessful(1000)) { (initial: BigInt) =>
      whenever(initial != 0) {
        assert {
          StreamExtensions
            .unfold(initial, x => if (x.abs < 1000) Optional.of(Pair.of(x.toString(), x * 2)) else Optional.empty())
            .toList.asScala.toList == List.unfold(initial)(x => if (x.abs < 1000) Some((x.toString(), x * 2)) else None)
        }
      }
    }
  }
}
