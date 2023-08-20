package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import io.github.kory33.guardedqueries.testutils.scalacheck.GenFormula
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import uk.ac.ox.cs.pdq.fol.Atom
import io.github.kory33.guardedqueries.testutils.scalacheck.GenSet
import uk.ac.ox.cs.pdq.fol.Variable

class ConjunctiveQueryExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  import io.github.kory33.guardedqueries.testutils.scalacheck.ShrinkFormula.given

  val largeCQ: Gen[ConjunctiveQuery] = GenFormula.genConjunctiveQuery(50, 10)

  // apply a given Int => Boolean function on the predicate number
  // on generated atoms
  def applyToPredicateNumber(f: Int => Boolean)(atom: Atom): Boolean = {
    val name = atom.getPredicate().getName()
    name.startsWith("P_") && f(name.drop(2).toInt)
  }

  ".filterAtom" should "only contain atoms in the input query" in {
    forAll(largeCQ, arbitrary[Int => Boolean], minSuccessful(3000)) { (cq, f) =>
      val result = ConjunctiveQueryExtensions.filterAtoms(cq, applyToPredicateNumber(f)(_))

      whenever(result.isPresent) {
        val filteredAtoms = result.get().getAtoms()
        assert(filteredAtoms.forall(atom => cq.getAtoms().contains(atom)))
      }
    }
  }

  ".filterAtom" should "contain all and only atoms that satisfy the predicate" in {
    forAll(largeCQ, arbitrary[Int => Boolean], minSuccessful(3000)) { (cq, f) =>
      val result = ConjunctiveQueryExtensions.filterAtoms(cq, applyToPredicateNumber(f)(_))
      val atomsInResult = result.map(_.getAtoms()).orElse(Array.empty).toList
      val atomsInInput = cq.getAtoms().toList

      assert {
        atomsInResult.forall { atom =>
          applyToPredicateNumber(f)(atom) == atomsInResult.contains(atom)
        }
      }
    }
  }

  ".filterAtom" should "not change variable boundedness" in {
    forAll(largeCQ, arbitrary[Int => Boolean], minSuccessful(3000)) { (cq, f) =>
      val result = ConjunctiveQueryExtensions.filterAtoms(cq, applyToPredicateNumber(f)(_))
      val atomsInResult = result.map(_.getAtoms()).orElse(Array.empty).toList
      val variablesInResult = atomsInResult.flatMap(_.getVariables().toList)

      assert {
        variablesInResult.forall(v =>
          cq.getBoundVariables().contains(v) == result.get().getBoundVariables().contains(v)
        )
      }
    }
  }

  val largeCQAndItsBoundVariables: Gen[(ConjunctiveQuery, Set[Variable])] =
    largeCQ.flatMap(cq =>
      GenSet.chooseSubset(cq.getBoundVariables().toSet).map((cq, _))
    )

  ".connectedComponents" should "cover the given set of query variables" in {
    forAll(largeCQAndItsBoundVariables, minSuccessful(3000)) {
      case (cq, variables) =>
        val atoms = cq.getAtoms().toList
        val components = ConjunctiveQueryExtensions.connectedComponents(cq, variables.asJava)

        assert {
          components.iterator().asScala.flatMap(_.asScala).toSet == variables
        }
    }
  }

  ".connectedComponents" should "partition the given set of query variables" in {
    forAll(largeCQAndItsBoundVariables, minSuccessful(3000)) {
      case (cq, variables) =>
        val atoms = cq.getAtoms().toList
        val components = ConjunctiveQueryExtensions.connectedComponents(cq, variables.asJava)
          .iterator().asScala
          .map(_.asScala.toSet)
          .toSet

        assert {
          components.forall(c1 => components.forall(c2 => c1 == c2 || c1.intersect(c2).isEmpty))
        }
    }
  }

  ".connectedComponents" should "put adjacent variables in the same component" in {
    forAll(largeCQAndItsBoundVariables, minSuccessful(3000)) {
      case (cq, inputVariableSet) =>
        val atoms = cq.getAtoms().toList
        val components =
          ConjunctiveQueryExtensions.connectedComponents(cq, inputVariableSet.asJava)
            .iterator().asScala
            .map(_.asScala.toSet)
            .toSet

        assert {
          cq.getAtoms().forall { atom =>
            val inputVariablesInAtom = atom.getVariables().toSet.intersect(inputVariableSet)

            inputVariablesInAtom.forall(v1 =>
              inputVariablesInAtom.forall(v2 =>
                components.exists(c => c.contains(v1) && c.contains(v2))
              )
            )
          }
        }
    }
  }

  ".connectedComponents" should "put two variables only if they are connected by a sequence of atoms" in {
    forAll(largeCQAndItsBoundVariables, minSuccessful(1000)) {
      case (cq, inputVariableSet) =>
        def atomsContainingAnyOf(variables: Set[Variable]): Set[Atom] =
          cq.getAtoms().toList.filter(
            _.getVariables().toSet.intersect(variables).nonEmpty
          ).toSet

        def variablesAdjacentTo(variables: Set[Variable]): Set[Variable] =
          atomsContainingAnyOf(variables).flatMap(_.getVariables().toSet).intersect(
            inputVariableSet
          )

        @scala.annotation.tailrec
        def variablesReachableFrom(variables: Set[Variable]): Set[Variable] = {
          val adjacentVariables = variablesAdjacentTo(variables)
          if (adjacentVariables == variables) variables
          else variablesReachableFrom(adjacentVariables)
        }

        val atoms = cq.getAtoms().toList
        val components =
          ConjunctiveQueryExtensions.connectedComponents(cq, inputVariableSet.asJava)
            .iterator().asScala
            .map(_.asScala.toSet)
            .toSet

        components.foreach { component =>
          component.foreach(v1 =>
            component.foreach(v2 =>
              assert(variablesReachableFrom(Set(v1)) == variablesReachableFrom(Set(v2)))
            )
          )
        }
    }
  }
}
