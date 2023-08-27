package io.github.kory33.guardedqueries.core.subsumption.formula

import com.google.common.collect.ImmutableList
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet.VariableOrConstant
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions
import io.github.kory33.guardedqueries.core.fol.DatalogRule
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin

import uk.ac.ox.cs.pdq.fol.{Constant => PDQConstant}

import java.util
import uk.ac.ox.cs.pdq.fol.Term
import uk.ac.ox.cs.pdq.fol.Atom
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance

/**
 * An implementation of {@link MaximallySubsumingTGDSet} that keeps track of a set of datalog
 * rules which are "maximal" with respect to the following subsumption relation: <p> A rule R1
 * subsumes a rule R2 (according to this implementation) if there exists a substitution {@code
 * s} mapping variables in A to variables and constants in B such that: <ol> <li>{@code
 * s(A.body)} is a subset of {@code B.body}</li> <li>{@code s(A.head)} is a superset to {@code
 * B.head}</li> </ol> Note that this relation indeed implies formula implication relation A ⊨ B.
 * <p> Proof: Suppose {@code A} and {@code t(B.body)} hold, where {@code t} is a substitution of
 * variables in {@code B.body} to elements in the domain of discourse. As {@code s(A.body)} is a
 * subset of {@code B.body}, {@code (t . s)(A.body)} holds. By {@code A}, {@code s(A.head)}
 * holds, and as this is a superset of {@code B.head}, {@code t(B.head)} holds. <hr> <p> Such a
 * substitution can be found by considering the body of {@code A} as a conjunctive query,
 * performing a join operation with it over the body of {@code B} (considered as a formal
 * instance of constants and variables) and then materializing the head of {@code A} to check
 * the supset condition.
 */
final class MinimallyUnifiedDatalogRuleSet
    extends IndexlessMaximallySubsumingTGDSet[DatalogRule] {

  override protected def firstRuleSubsumesSecond(
    first: DatalogRule,
    second: DatalogRule
  ): Boolean = {
    val joinAlgorithm =
      new FilterNestedLoopJoin(VariableOrConstant.Constant(_))

    joinAlgorithm.join(
      TGDExtensions.bodyAsCQ(first),
      MinimallyUnifiedDatalogRuleSet.atomArrayIntoFormalInstance(second.getBodyAtoms)
    ).allHomomorphisms.stream.anyMatch(homomorphism => {
      val substitutedFirstHead = homomorphism.materializeFunctionFreeAtoms(
        util.Arrays.asList(first.getHeadAtoms: _*),
        MinimallyUnifiedDatalogRuleSet.VariableOrConstant.Constant(_)
      )
      val secondHead =
        MinimallyUnifiedDatalogRuleSet.atomArrayIntoFormalInstance(second.getHeadAtoms)

      substitutedFirstHead.isSuperInstanceOf(secondHead)
    })
  }
}

object MinimallyUnifiedDatalogRuleSet {
  enum VariableOrConstant {
    case Variable(variable: Variable)
    case Constant(constant: PDQConstant)
  }

  object VariableOrConstant {
    def of(term: Term): VariableOrConstant =
      term match
        case variable: Variable    => VariableOrConstant.Variable(variable)
        case constant: PDQConstant => VariableOrConstant.Constant(constant)
        case _ =>
          throw new IllegalArgumentException("Either a constant or a variable is expected")
  }

  private def atomIntoFormalFact(atom: Atom) = {
    val appliedTerms = ImmutableList.copyOf(
      util.Arrays.stream(atom.getTerms).map(VariableOrConstant.of).iterator
    )

    FormalFact(atom.getPredicate, appliedTerms)
  }

  private def atomArrayIntoFormalInstance(atoms: Array[Atom]) = FormalInstance.fromIterator(
    util.Arrays.stream(atoms).map(MinimallyUnifiedDatalogRuleSet.atomIntoFormalFact).iterator
  )
}
