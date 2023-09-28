package io.github.kory33.guardedqueries.core.subsumption.formula

import io.github.kory33.guardedqueries.core.fol.DatalogRule
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.IncludesFolConstants
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet.VariableOrConstant
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Term
import uk.ac.ox.cs.pdq.fol.{Constant => PDQConstant}
import uk.ac.ox.cs.pdq.fol.{Variable => PDQVariable}

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
  import VariableOrConstant.given

  override protected def firstRuleSubsumesSecond(
    first: DatalogRule,
    second: DatalogRule
  ): Boolean = {
    val secondBodyInstance = second.getBodyAtoms.intoFormalInstanceOfVariableOrConstant

    FilterNestedLoopJoin[PDQVariable, VariableOrConstant]
      .joinConjunctiveQuery(first.bodyAsCQ, secondBodyInstance)
      .allHomomorphisms
      .exists(homomorphism => {
        val substitutedFirstHead =
          homomorphism.materializeFunctionFreeAtoms(first.getHeadAtoms.toSet)
        val secondHead =
          second.getHeadAtoms.intoFormalInstanceOfVariableOrConstant

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

    private[MinimallyUnifiedDatalogRuleSet] given AtomExtensions: AnyRef with {
      extension (atom: Atom)
        private def intoFormalFactOfVariableOrConstant: FormalFact[VariableOrConstant] =
          FormalFact(atom.getPredicate, atom.getTerms.map(VariableOrConstant.of).toList)

      extension (atoms: Array[Atom])
        def intoFormalInstanceOfVariableOrConstant: FormalInstance[VariableOrConstant] =
          FormalInstance(atoms.map(_.intoFormalFactOfVariableOrConstant).toSet)
    }
  }
}
