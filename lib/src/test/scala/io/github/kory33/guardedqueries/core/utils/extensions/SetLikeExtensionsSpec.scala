package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SetLikeExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  ".union" should "be equivalent to Scala's union" in {
    forAll(minSuccessful(1000)) { (xs: Set[Int], ys: Set[Int]) =>
      assert(SetLikeExtensions.union(xs.asJava, ys.asJava) == xs.union(ys))
    }
  }

  ".intersection" should "be equivalent to Scala's intersect" in {
    forAll(minSuccessful(1000)) { (xs: Set[Int], ys: Set[Int]) =>
      assert(SetLikeExtensions.intersection(xs.asJava, ys.asJava) == xs.intersect(ys))
    }
  }

  ".nontriviallyIntersects" should "be equivalent to Scala's intersect.nonEmpty" in {
    forAll(minSuccessful(1000)) { (xs: Set[Int], ys: Set[Int]) =>
      assert(SetLikeExtensions.nontriviallyIntersects(xs.asJava, ys.asJava) == xs.intersect(
        ys
      ).nonEmpty)
    }
  }

  ".disjoint" should "be equivalent to Scala's intersect.isEmpty" in {
    forAll(minSuccessful(1000)) { (xs: Set[Int], ys: Set[Int]) =>
      assert(SetLikeExtensions.disjoint(xs.asJava, ys.asJava) == xs.intersect(ys).isEmpty)
    }
  }

  ".difference" should "be equivalent to Scala's diff" in {
    forAll(minSuccessful(1000)) { (xs: Set[Int], ys: Set[Int]) =>
      assert(SetLikeExtensions.difference(xs.asJava, ys.asJava) == xs.diff(ys))
    }
  }

  val smallSet: Gen[Set[Int]] = Gen.chooseNum(0, 12).map(n => (1 to n).toSet)

  ".powerset" should "contain 2^|input| sets" in {
    forAll(smallSet) { (xs: Set[Int]) =>
      assert(SetLikeExtensions.powerset(xs.asJava).iterator().size == Math.pow(
        2,
        xs.size
      ))
    }
  }

  ".powerset" should "only produce subsets of input set" in {
    forAll(smallSet) { (xs: Set[Int]) =>
      assert {
        SetLikeExtensions.powerset(xs.asJava).iterator().forall { set => set.subsetOf(xs) }
      }
    }
  }

  // A test function that will be used for fixpoint computation tests
  val simpleGeneratorFunction: BigInt => java.util.Set[BigInt] = x =>
    Set(x * 2, x * 3)
      // we need to set some termination condition
      // or else the fixpoint will be infinite
      .filter(y => y.abs < 10000)
      .asJava

  ".generateFromElementsUntilFixpoint" should "output a set containing the initial set" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = SetLikeExtensions.generateFromElementsUntilFixpoint(
        xs.asJava,
        simpleGeneratorFunction
      )
      assert(xs.subsetOf(generatedSet))
    }
  }

  ".generateFromElementsUntilFixpoint" should "output a fixpoint of the function" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = SetLikeExtensions.generateFromElementsUntilFixpoint(
        xs.asJava,
        simpleGeneratorFunction
      ).toSet
      assert(generatedSet == (generatedSet union generatedSet.flatMap(
        simpleGeneratorFunction.apply(_)
      )))
    }
  }

  ".generateFromElementsUntilFixpoint" should "be equivalent to naive least fixed point computation" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      @scala.annotation.tailrec
      def leastFixedPointNaively[T](s: Set[T], fn: T => Set[T]): Set[T] = {
        val newSet = s union s.flatMap(fn)
        if (newSet == s) s else leastFixedPointNaively(newSet, fn)
      }

      val generatedSet = SetLikeExtensions.generateFromElementsUntilFixpoint(
        xs.asJava,
        simpleGeneratorFunction
      )
      val expected = leastFixedPointNaively(xs, simpleGeneratorFunction.apply(_).toSet)

      assert(generatedSet == expected)
    }
  }

  val simpleSetGeneratorFunction: java.util.Collection[BigInt] => java.util.Set[BigInt] = set =>
    new java.util.HashSet(
      set.flatMap(simpleGeneratorFunction.apply(_)).asJavaCollection
    )

  ".generateFromSetUntilFixpoint" should "output a set containing the initial set" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = SetLikeExtensions.generateFromSetUntilFixpoint(
        xs.asJava,
        simpleSetGeneratorFunction
      )
      assert(xs.subsetOf(generatedSet))
    }
  }

  ".generateFromSetUntilFixpoint" should "output a fixpoint of the function" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = SetLikeExtensions.generateFromSetUntilFixpoint(
        xs.asJava,
        simpleSetGeneratorFunction
      ).toSet
      assert(generatedSet == (generatedSet union simpleSetGeneratorFunction(
        generatedSet.asJava
      )))
    }
  }

  ".generateFromSetUntilFixpoint" should "be equivalent to naive least fixed point computation" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      @scala.annotation.tailrec
      def leastFixedPointNaively[T](s: Set[T], fn: Set[T] => Set[T]): Set[T] = {
        val newSet = s union fn(s)
        if (newSet == s) s else leastFixedPointNaively(newSet, fn)
      }

      val generatedSet = SetLikeExtensions.generateFromSetUntilFixpoint(
        xs.asJava,
        simpleSetGeneratorFunction
      )
      val expected = leastFixedPointNaively(
        xs,
        s => simpleSetGeneratorFunction.apply(s.asJava).toSet
      )

      assert(generatedSet == expected)
    }
  }
}
