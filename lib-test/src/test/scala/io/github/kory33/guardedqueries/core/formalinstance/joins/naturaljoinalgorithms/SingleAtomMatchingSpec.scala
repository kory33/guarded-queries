package io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import io.github.kory33.guardedqueries.testutils.scalacheck.GenFormula
import io.github.kory33.guardedqueries.testutils.scalacheck.GenSet
import io.github.kory33.guardedqueries.testutils.scalacheck.utils.TraverseListGen.traverse
import uk.ac.ox.cs.pdq.fol.Predicate
import io.github.kory33.guardedqueries.testutils.scalacheck.GenFormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Atom
import io.github.kory33.guardedqueries.core.formalinstance.joins.SingleAtomMatching

class SingleAtomMatchingSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  val genSmallAtom = GenFormula.genAtom(
    4,
    Gen.oneOf(GenFormula.genNumberedVariable(30), GenFormula.genConstant(4))
  )

  val genSignatureAndFormalInstance: Gen[(Set[Predicate], FormalInstance[Constant])] = for {
    predicateCount <- Gen.chooseNum(1, 10)
    predicates <- (1 to predicateCount).toList.traverse { index =>
      Gen.chooseNum(1, 4).map { arity => Predicate.create(s"P_${index}", arity) }
    }
    predicateSet = predicates.toSet
    instance <- GenFormalInstance.genFormalInstanceContainingPredicates(predicateSet) 
  } yield (predicateSet, instance)

  val genInstanceAndQuery: Gen[(FormalInstance[Constant], Atom)] =
    genSignatureAndFormalInstance.flatMap { case (predicateSet, instance) =>
      for {
        predicate <- Gen.oneOf(predicateSet)
        terms <- Gen.listOfN(predicate.getArity(), Gen.oneOf(GenFormula.genNumberedVariable(4), GenFormula.genConstant(4)))
      } yield (instance, Atom.create(predicate, terms*))
    }

  "SingleAtomMatching.allMatches.materializeFunctionFreeAtom" should "only include formal facts having the same atom as query atom" in {
    forAll(genInstanceAndQuery, minSuccessful(100)) { case (instance, query) =>
      SingleAtomMatching
        .allMatches(query, instance, c => c)
        .materializeFunctionFreeAtom(query, c => c)
        .asScala
        .foreach { fact =>
          assert(fact.predicate().equals(query.getPredicate()))
        }
    }
  }

  it should "be a subset of input instance" in {
    forAll(genInstanceAndQuery, minSuccessful(100)) { case (instance, query) =>
      SingleAtomMatching
        .allMatches(query, instance, c => c)
        .materializeFunctionFreeAtom(query, c => c)
        .asScala
        .foreach { fact => assert(instance.facts.contains(fact)) }
    }
  }

  it should "be idempotent" in {
    forAll(genInstanceAndQuery, minSuccessful(100)) { case (instance, query) =>
      val firstMatch = SingleAtomMatching
          .allMatches(query, instance, c => c)
          .materializeFunctionFreeAtom(query, c => c)
      
      val secondMatch = SingleAtomMatching
        .allMatches(query, new FormalInstance(firstMatch), c => c)
        .materializeFunctionFreeAtom(query, c => c)

      assert(firstMatch.asScala.toSet == secondMatch.asScala.toSet)
    }
  }

  it should "find every valid answer" in {
    // TODO: the test should look like the following:
    // forAll(atomAndHomomorphism, minSuccessful(100)) { case (atom, homomorphism) =>
    //   val instance = new FormalInstance(atom mapped with homomorphism)
    //   SingleAtomMatching
    //     .allMatches(atom, instance, c => c).
    //     .materializeFunctionFreeAtom(atom, c => c)
    //     // should contain a single homomorphism that results in the same tuple as the original homomorphism
  }
}
