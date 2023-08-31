package io.github.kory33.guardedqueries.core.utils

import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap
import org.scalacheck.Gen.*
import org.scalacheck.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class FunctionSpacesSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  val smallSetSize: Gen[Int] = Gen.chooseNum(0, 5)
  def setOfSize(size: Int): Set[Int] = (1 to size).toSet

  def respectsDomainAndCodomain(
    map: Map[Int, Int],
    domain: Set[Int],
    codomain: Set[Int]
  ): Boolean =
    map.keys.forall(domain.contains) && map.values.forall(codomain.contains)

  ".allTotalFunctionsBetween" should "produce maps that respect domain and codomain" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val domain = setOfSize(domainSize)
      val codomain = setOfSize(codomainSize)

      val allFunctions = FunctionSpaces.allTotalFunctionsBetween(domain, codomain)

      assert(allFunctions.forall(respectsDomainAndCodomain(_, domain, codomain)))
    }
  }

  ".allTotalFunctionsBetween" should "enumerate |codomain|^|domain| functions" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val allFunctions = FunctionSpaces
        .allTotalFunctionsBetween(setOfSize(domainSize), setOfSize(codomainSize))

      assert(allFunctions.size == {
        if (codomainSize == 0 && domainSize == 0)
          1
        else
          Math.pow(codomainSize, domainSize).toInt
      })
    }
  }

  ".allPartialFunctionsBetween" should "produce maps that respect domain and codomain" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val domain = setOfSize(domainSize)
      val codomain = setOfSize(codomainSize)

      val allFunctions = FunctionSpaces.allPartialFunctionsBetween(domain, codomain)

      assert(allFunctions.forall(respectsDomainAndCodomain(_, domain, codomain)))
    }
  }

  ".allPartialFunctionsBetween" should "enumerate |codomain+1|^|domain| functions" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val allFunctions = FunctionSpaces.allPartialFunctionsBetween(
        setOfSize(domainSize),
        setOfSize(codomainSize)
      )

      assert(allFunctions.size == Math.pow(codomainSize + 1, domainSize).toInt)
    }
  }

  ".allInjectiveTotalFunctionsBetween" should "produce maps that respect domain and codomain" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val domain = setOfSize(domainSize)
      val codomain = setOfSize(codomainSize)

      val allFunctions = FunctionSpaces.allInjectiveTotalFunctionsBetween(domain, codomain)

      assert(allFunctions.forall(respectsDomainAndCodomain(_, domain, codomain)))
    }
  }

  ".allInjectiveTotalFunctionsBetween" should "enumerate all injections" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val enumeratedInjectiveTotalFunctions = FunctionSpaces
        .allInjectiveTotalFunctionsBetween(setOfSize(domainSize), setOfSize(codomainSize))
        .toSet

      val allTotalFunctions = FunctionSpaces
        .allTotalFunctionsBetween(setOfSize(domainSize), setOfSize(codomainSize))
        .toSet

      val allInjectiveTotalFunctions = allTotalFunctions
        .filter(function => function.keys.size == function.values.toSet.size)
        .map(BijectiveMap.tryFromInjectiveMap(_).get)

      assert { enumeratedInjectiveTotalFunctions == allInjectiveTotalFunctions }
    }
  }
}
