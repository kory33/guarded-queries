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
import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import com.google.common.collect.ImmutableList
import io.github.kory33.guardedqueries.core.formalinstance.joins.JoinResult

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

  def genAtomAndHomomorphism: Gen[(Atom, HomomorphicMapping[Constant])] = for {
    atom <- genSmallAtom
    variablesInAtom = atom.getVariables().toSet.toList
    homomorphism <- Gen.listOfN(variablesInAtom.size, GenFormula.genConstant(10))
  } yield (atom, new HomomorphicMapping[Constant](ImmutableList.copyOf(variablesInAtom.asJava), ImmutableList.copyOf(homomorphism.asJava)))

  it should "find every valid answer" in {
    // We test that for every pair of query and a homomorphism,
    // the algorithm successfully finds the mapped tuple as the answer.
    //
    // For instance, for an atomic query A(x,c) we might prepare a homomorphism {x -> d}.
    // When we run the matching algorithm on the instance {A(d,c)}, which is the result of
    // materializing the atom according to the homomorphism, we expect the algorithm to
    // return {x -> d} as the only answer, and that the instance materialized from the answer
    // is equal to {A(d,c)}.
    // 
    // Assuming that the algorithm is monotonic and affine in the input instance (i.e. the
    // presence of a homomorphism in the answer only depends on the existence of the corresponding
    // tuple in the input instance), this test should be sufficient to show the correctness of
    // the algorithm.
    forAll(genAtomAndHomomorphism, minSuccessful(1000)) { case (atom, homomorphism) =>
      val instanceContainingJustTheMaterializedAtom = FormalInstance.of(homomorphism.materializeFunctionFreeAtom(atom, c => c))
      val matches = SingleAtomMatching
        .allMatches(atom, instanceContainingJustTheMaterializedAtom, c => c)
        .materializeFunctionFreeAtom(atom, c => c)
      val matchInstance = new FormalInstance(matches)
      
      assert(matchInstance == instanceContainingJustTheMaterializedAtom)
    }
  }
}
