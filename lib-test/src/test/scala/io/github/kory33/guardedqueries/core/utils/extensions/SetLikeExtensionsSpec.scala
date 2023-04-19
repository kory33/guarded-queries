package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*

object SetLikeExtensionsSpec extends Properties("SetLikeExtensions") {
  import Prop.forAll

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(1000)

  property("union should be equivalent to Scala's union") = forAll { (xs: Set[Int], ys: Set[Int]) =>
    SetLikeExtensions.union(xs.asJava, ys.asJava).asScala == xs.union(ys)
  }

  property("intersection should be equivalent to Scala's intersect") = forAll { (xs: Set[Int], ys: Set[Int]) =>
    SetLikeExtensions.intersection(xs.asJava, ys.asJava).asScala == xs.intersect(ys)
  }

  property("nontriviallyIntersects should be equivalent to Scala's intersect.nonEmpty") = forAll { (xs: Set[Int], ys: Set[Int]) =>
    SetLikeExtensions.nontriviallyIntersects(xs.asJava, ys.asJava) == xs.intersect(ys).nonEmpty
  }

  property("disjoint should be equivalent to Scala's intersect.isEmpty") = forAll { (xs: Set[Int], ys: Set[Int]) =>
    SetLikeExtensions.disjoint(xs.asJava, ys.asJava) == xs.intersect(ys).isEmpty
  }

  property("difference should be equivalent to Scala's diff") = forAll { (xs: Set[Int], ys: Set[Int]) =>
    SetLikeExtensions.difference(xs.asJava, ys.asJava).asScala == xs.diff(ys)
  }

  val smallSet = Gen.chooseNum(0, 12).map(n => (1 to n).toSet)

  property("powerset should contain 2^|input| sets") = forAll(smallSet) { (xs: Set[Int]) =>
    SetLikeExtensions.powerset(xs.asJava).iterator().asScala.size == Math.pow(2, xs.size)
  }

  property("powerset should only produce subsets of input set") = forAll(smallSet) { (xs: Set[Int]) =>
    SetLikeExtensions.powerset(xs.asJava).iterator().asScala.forall { set =>
      set.asScala.subsetOf(xs)
    }
  }

  // A test function that will be used for fixpoint computation tests
  val simpleGeneratorFunction: java.util.function.Function[BigInt, java.util.Set[BigInt]] = x =>
    Set(x * 2, x * 3)
      // we need to set some termination condition
      // or else the fixpoint will be infinite
      .filter(y => y.abs < 10000)
      .asJava

  property("generateFromElementsUntilFixpoint should output a set containing the initial set") = forAll { (xs: Set[BigInt]) =>
    val generatedSet = SetLikeExtensions.generateFromElementsUntilFixpoint(xs.asJava, simpleGeneratorFunction).asScala
    xs.subsetOf(generatedSet)
  }

  property("generateFromElementsUntilFixpoint should output a fixpoint of the function") = forAll { (xs: Set[BigInt]) =>
    val generatedSet = SetLikeExtensions.generateFromElementsUntilFixpoint(xs.asJava, simpleGeneratorFunction).asScala.toSet
    generatedSet == (generatedSet union (generatedSet.flatMap(simpleGeneratorFunction.apply(_).asScala)))
  }

  property("generateFromElementsUntilFixpoint should be equivalent to naive least fixed point computation") = forAll { (xs: Set[BigInt]) =>
    @scala.annotation.tailrec
    def leastFixedPointNaively[T](s: Set[T], fn: T => Set[T]): Set[T] = {
      val newSet = s union s.flatMap(fn)
      if (newSet == s) s else leastFixedPointNaively(newSet, fn)
    }

    val generatedSet = SetLikeExtensions.generateFromElementsUntilFixpoint(xs.asJava, simpleGeneratorFunction).asScala
    val expected = leastFixedPointNaively(xs, simpleGeneratorFunction.apply(_).asScala.toSet)

    generatedSet == expected
  }

  val simpleSetGeneratorFunction: java.util.function.Function[java.util.Collection[BigInt], java.util.Set[BigInt]] = set =>
    new java.util.HashSet(set.asScala.flatMap(simpleGeneratorFunction.apply(_).asScala).asJavaCollection)

  property("generateFromSetUntilFixpoint should output a set containing the initial set") = forAll { (xs: Set[BigInt]) =>
    val generatedSet = SetLikeExtensions.generateFromSetUntilFixpoint(xs.asJava, simpleSetGeneratorFunction).asScala
    xs.subsetOf(generatedSet)
  }

  property("generateFromSetUntilFixpoint should output a fixpoint of the function") = forAll { (xs: Set[BigInt]) =>
    val generatedSet = SetLikeExtensions.generateFromSetUntilFixpoint(xs.asJava, simpleSetGeneratorFunction).asScala.toSet
    generatedSet == (generatedSet union simpleSetGeneratorFunction(generatedSet.asJava).asScala)
  }

  property("generateFromSetUntilFixpoint should be equivalent to naive least fixed point computation") = forAll { (xs: Set[BigInt]) =>
    @scala.annotation.tailrec
    def leastFixedPointNaively[T](s: Set[T], fn: Set[T] => Set[T]): Set[T] = {
      val newSet = s union fn(s)
      if (newSet == s) s else leastFixedPointNaively(newSet, fn)
    }

    val generatedSet = SetLikeExtensions.generateFromSetUntilFixpoint(xs.asJava, simpleSetGeneratorFunction).asScala
    val expected = leastFixedPointNaively(xs, s => simpleSetGeneratorFunction.apply(s.asJava).asScala.toSet)

    generatedSet == expected
  }
}
