package io.github.kory33.guardedqueries.core.subsumption.formula

import io.github.kory33.guardedqueries.core.fol.DatalogRule
import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance, IncludesFolConstants}
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet.VariableOrConstant
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Term
import uk.ac.ox.cs.pdq.fol.Constant as PDQConstant
import uk.ac.ox.cs.pdq.fol.Variable as PDQVariable
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given

/**
 * An implementation of [[MaximallySubsumingTGDSet]] that keeps track of a set of datalog rules
 * which are "maximal" with respect to the following subsumption relation:
 *
 * A rule R1 subsumes a rule R2 (according to this implementation) if there exists a
 * substitution `s` mapping variables in A to variables and constants in B such that: <ol>
 * <li>`s(A.body)` is a subset of `B.body`</li> <li>`s(A.head)` is a superset to `B.head`</li>
 * </ol> Note that this relation indeed implies formula implication relation A ⊨ B.
 *
 * Proof: Suppose `A` and `t(B.body)` hold, where `t` is a substitution of variables in `B.body`
 * to elements in the domain of discourse. As `s(A.body)` is a subset of `B.body`,
 * `(t.s)(A.body)` holds. By `A`, `s(A.head)` holds, and as this is a superset of `B.head`,
 * `t(B.head)` holds. <hr>
 *
 * Such a substitution can be found by considering the body of `A` as a conjunctive query,
 * performing a join operation with it over the body of `B` (considered as a formal instance of
 * constants and variables) and then materializing the head of `A` to check the supset
 * condition.
 */
final class MinimallyUnifiedDatalogRuleSet
    extends IndexlessMaximallySubsumingTGDSet[DatalogRule] {

  override protected def firstRuleSubsumesSecond(
    first: DatalogRule,
    second: DatalogRule
  ): Boolean = {
    FilterNestedLoopJoin[VariableOrConstant].join(
      first.bodyAsCQ,
      MinimallyUnifiedDatalogRuleSet.atomArrayIntoFormalInstance(second.getBodyAtoms)
    ).allHomomorphisms.exists(homomorphism => {
      val substitutedFirstHead =
        homomorphism.materializeFunctionFreeAtoms(first.getHeadAtoms.toSet)
      val secondHead =
        MinimallyUnifiedDatalogRuleSet.atomArrayIntoFormalInstance(second.getHeadAtoms)

      substitutedFirstHead.isSuperInstanceOf(secondHead)
    })
  }
}

object MinimallyUnifiedDatalogRuleSet {
  enum VariableOrConstant {
    case Variable(variable: PDQVariable)
    case Constant(constant: PDQConstant)
  }

  object VariableOrConstant {
    def of(term: Term): VariableOrConstant =
      term match
        case variable: PDQVariable => VariableOrConstant.Variable(variable)
        case constant: PDQConstant => VariableOrConstant.Constant(constant)
        case _ =>
          throw new IllegalArgumentException("Either a constant or a variable is expected")

    given IncludesFolConstants[VariableOrConstant] with {
      override def includeConstant(constant: PDQConstant): VariableOrConstant =
        VariableOrConstant.Constant(constant)
    }
  }

  private def atomIntoFormalFact(atom: Atom) =
    FormalFact(
      atom.getPredicate,
      atom.getTerms.map(VariableOrConstant.of).toList
    )

  private def atomArrayIntoFormalInstance(atoms: Array[Atom]) = FormalInstance(
    atoms.map(atomIntoFormalFact).toSet
  )
}
