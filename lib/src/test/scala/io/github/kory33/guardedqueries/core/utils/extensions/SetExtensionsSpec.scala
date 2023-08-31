package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.Gen.*
import org.scalacheck.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SetExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import SetExtensions.given

  val smallSet: Gen[Set[Int]] = Gen.chooseNum(0, 12).map(n => (1 to n).toSet)

  ".powerset" should "contain 2^|input| sets" in {
    forAll(smallSet) { (xs: Set[Int]) => assert(xs.powerset.size == Math.pow(2, xs.size)) }
  }

  ".powerset" should "only produce subsets of input set" in {
    forAll(smallSet) { (xs: Set[Int]) =>
      assert { xs.powerset.forall { set => set.subsetOf(xs) } }
    }
  }

  // A test function that will be used for fixpoint computation tests
  val simpleGeneratorFunction: BigInt => Set[BigInt] = x =>
    Set(x * 2, x * 3)
      // we need to set some termination condition
      // or else the fixpoint will be infinite
      .filter(y => y.abs < 10000)

  ".generateFromElementsUntilFixpoint" should "output a set containing the initial set" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = xs.generateFromElementsUntilFixpoint(simpleGeneratorFunction)
      assert(xs.subsetOf(generatedSet))
    }
  }

  ".generateFromElementsUntilFixpoint" should "output a fixpoint of the function" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = xs.generateFromElementsUntilFixpoint(simpleGeneratorFunction)
      assert(generatedSet == (generatedSet union generatedSet.flatMap(
        simpleGeneratorFunction
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

      val generatedSet = xs.generateFromElementsUntilFixpoint(simpleGeneratorFunction)
      val expected = leastFixedPointNaively(xs, simpleGeneratorFunction(_).toSet)

      assert(generatedSet == expected)
    }
  }

  val simpleSetGeneratorFunction: Set[BigInt] => Set[BigInt] =
    _.flatMap(simpleGeneratorFunction)

  ".generateFromSetUntilFixpoint" should "output a set containing the initial set" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = xs.generateFromSetUntilFixpoint(simpleSetGeneratorFunction)
      assert(xs.subsetOf(generatedSet))
    }
  }

  ".generateFromSetUntilFixpoint" should "output a fixpoint of the function" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      val generatedSet = xs.generateFromSetUntilFixpoint(simpleSetGeneratorFunction)
      assert(generatedSet == (generatedSet union simpleSetGeneratorFunction(generatedSet)))
    }
  }

  ".generateFromSetUntilFixpoint" should "be equivalent to naive least fixed point computation" in {
    forAll(minSuccessful(1000)) { (xs: Set[BigInt]) =>
      @scala.annotation.tailrec
      def leastFixedPointNaively[T](s: Set[T], fn: Set[T] => Set[T]): Set[T] = {
        val newSet = s union fn(s)
        if (newSet == s) s else leastFixedPointNaively(newSet, fn)
      }

      val generatedSet = xs.generateFromSetUntilFixpoint(simpleSetGeneratorFunction)
      val expected = leastFixedPointNaively(xs, s => simpleSetGeneratorFunction(s))

      assert(generatedSet == expected)
    }
  }
}
