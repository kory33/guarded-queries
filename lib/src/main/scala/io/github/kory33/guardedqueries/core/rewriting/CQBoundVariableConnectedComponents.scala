package io.github.kory33.guardedqueries.core.rewriting

import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Variable
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given

// format: off
/**
 * A class of objects representing the decomposition of a conjunctive query into
 * bound-variable-connected components. The decomposition consists of bound-variable-free atoms
 * and maximally bound-variable-connected subqueries.
 *
 * The query input to the [[GuardedRuleAndQueryRewriter]] is first "split" into
 * bound-variable-connected components using this class, whose components are rewritten
 * separately and then combined (by the "subgoal binding rule") to derive the final goal atom.
 *
 * For example, given a conjunctive query
 *
 * <pre> ∃x,y,z. U(c) ∧ U(w) ∧ U(x) ∧ R(x,w) ∧ T(z,c,w) ∧ R(y,z) </pre>
 *
 * with a free variable `w` and a constant `c`,
 *   - the set of bound-variable-free atoms is { U(c), U(w) }, and
 *   - the set of maximally (bound-variable-) connected subqueries is
 *     `{ ∃x. U(x) ∧ R(x,w), ∃y,z. T(z,c,w) ∧ R(y,z)}`.
 *
 * Note that the atom `R(x,w)` and the atom `T(z,c,w)` are not bound-variable-connected
 * (therefore separated into different subqueries) because the variable `w` is free in the CQ.
 *
 * The constructor takes a [[ConjunctiveQuery]] object and computes the decomposition.
 */
// format: on
class CQBoundVariableConnectedComponents(cq: ConjunctiveQuery) {
  private val cqBoundVariables: Set[Variable] = cq.getBoundVariables.toSet

  val boundVariableFreeAtoms: Set[Atom] =
    cq.getAtoms.filter(atom => atom.getVariables.forall(!cqBoundVariables.contains(_))).toSet

  val maximallyConnectedSubqueries: Set[ConjunctiveQuery] = {
    // split bound variables into connected components
    val boundVariableConnectedComponents: Set[Set[Variable]] = {
      val boundVariableUFTree = SimpleUnionFindTree(cqBoundVariables)

      for (atom <- cq.getAtoms) {
        boundVariableUFTree.unionAll(atom.getVariables.toSet.intersect(cqBoundVariables))
      }

      boundVariableUFTree.getEquivalenceClasses
    }

    boundVariableConnectedComponents.map(component =>
      // this .get() call succeeds because
      // we are strictly inducing a subquery by
      // a maximally connected component of bound variables
      cq.strictlyInduceSubqueryByVariables(component).get
    )
  }
}
