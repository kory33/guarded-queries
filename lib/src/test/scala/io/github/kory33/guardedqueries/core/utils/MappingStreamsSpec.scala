package io.github.kory33.guardedqueries.core.utils

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import com.google.common.collect.ImmutableBiMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MappingStreamsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  val smallSetSize: Gen[Int] = Gen.chooseNum(0, 5)
  def javaSetOfSize(size: Int): Set[Integer] =
    Set.copyOf((1 to size).map(Integer.valueOf).asJava)

  def respectsDomainAndCodomain(
    javaMap: java.util.Map[Integer, Integer],
    domain: java.util.Set[Integer],
    codomain: java.util.Set[Integer]
  ): Boolean =
    javaMap.keySet().forall(domain.contains) &&
      javaMap.values().forall(codomain.contains)

  ".allTotalFunctionsBetween" should "produce maps that respect domain and codomain" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val domain = javaSetOfSize(domainSize)
      val codomain = javaSetOfSize(codomainSize)

      val allFunctions =
        MappingStreams.allTotalFunctionsBetween(domain, codomain).iterator()

      assert(allFunctions.forall(respectsDomainAndCodomain(_, domain, codomain)))
    }
  }

  ".allTotalFunctionsBetween" should "enumerate |codomain|^|domain| functions" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val allFunctions = MappingStreams
        .allTotalFunctionsBetween(javaSetOfSize(domainSize), javaSetOfSize(codomainSize))
        .iterator().toSet

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
      val domain = javaSetOfSize(domainSize)
      val codomain = javaSetOfSize(codomainSize)

      val allFunctions =
        MappingStreams.allPartialFunctionsBetween(domain, codomain).iterator()

      assert(allFunctions.forall(respectsDomainAndCodomain(_, domain, codomain)))
    }
  }

  ".allPartialFunctionsBetween" should "enumerate |codomain+1|^|domain| functions" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val allFunctions = MappingStreams
        .allPartialFunctionsBetween(javaSetOfSize(domainSize), javaSetOfSize(codomainSize))
        .iterator().toSet

      assert(allFunctions.size == Math.pow(codomainSize + 1, domainSize).toInt)
    }
  }

  ".allInjectiveTotalFunctionsBetween" should "produce maps that respect domain and codomain" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val domain = javaSetOfSize(domainSize)
      val codomain = javaSetOfSize(codomainSize)

      val allFunctions =
        MappingStreams.allInjectiveTotalFunctionsBetween(domain, codomain).iterator()

      assert(allFunctions.forall(respectsDomainAndCodomain(_, domain, codomain)))
    }
  }

  ".allInjectiveTotalFunctionsBetween" should "enumerate all injections" in {
    forAll(smallSetSize, smallSetSize) { (domainSize: Int, codomainSize: Int) =>
      val allInjectiveTotalFunctions = MappingStreams
        .allInjectiveTotalFunctionsBetween(
          javaSetOfSize(domainSize),
          javaSetOfSize(codomainSize)
        )
        .iterator().toSet

      val allTotalFunctions = MappingStreams
        .allTotalFunctionsBetween(javaSetOfSize(domainSize), javaSetOfSize(codomainSize))
        .iterator().toSet

      assert {
        allTotalFunctions
          .filter(function =>
            function.keySet().size == function.entrySet().map(_.getValue()).toSet.size
          )
          .forall(function =>
            allInjectiveTotalFunctions.contains(ImmutableBiMap.copyOf(function))
          )
      }
    }
  }
}
