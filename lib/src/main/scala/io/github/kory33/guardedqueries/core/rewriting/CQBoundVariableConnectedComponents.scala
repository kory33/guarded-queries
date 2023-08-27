package io.github.kory33.guardedqueries.core.rewriting

import com.google.common.collect.ImmutableSet
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Variable
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery

import java.util
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions

/**
 * A class of objects representing the decomposition of a conjunctive query into
 * bound-variable-connected components. The decomposition consists of bound-variable-free atoms
 * and maximally bound-variable-connected subqueries. <p> The query input to the {@link
 * GuardedRuleAndQueryRewriter} is first "split" into bound-variable-connected components using
 * this class, whose components are rewritten separately and then combined (by the "subgoal
 * binding rule") to derive the final goal atom. <p> For example, given a conjunctive query
 * {@code ∃x,y,z. U(c) ∧ U(w) ∧ U(x) ∧ R(x,w) ∧ T(z,c,w) ∧ R(y,z)} with a free variable {@code
 * w} and a constant {@code c}, <ul> <li>the set of bound-variable-free atoms is { U(c), U(w) },
 * and </li> <li> the set of maximally (bound-variable-) connected subqueries is {@code { ∃x.
 * U(x) ∧ R(x,w), ∃y,z. T(z,c,w) ∧ R(y,z) }}. </li> </ul> Note that the atom {@code R(x,w)} and
 * the atom {@code T(z,c,w)} are not bound-variable-connected (therefore separated into
 * different subqueries) because the variable {@code w} is free in the CQ. <p> The constructor
 * takes a {@link ConjunctiveQuery} object and computes the decomposition.
 */
class CQBoundVariableConnectedComponents(cq: ConjunctiveQuery) {
  private val cqBoundVariables: ImmutableSet[Variable] =
    ImmutableSet.copyOf(cq.getBoundVariables)

  val boundVariableFreeAtoms: ImmutableSet[Atom] =
    ImmutableSet.copyOf(util.Arrays.stream(cq.getAtoms).filter((atom: Atom) =>
      util.Arrays.stream(atom.getVariables).noneMatch(cqBoundVariables.contains)
    ).iterator)

  val maximallyConnectedSubqueries: ImmutableSet[ConjunctiveQuery] = {
    // split bound variables into connected components
    val boundVariableUFTree = new SimpleUnionFindTree(cqBoundVariables)
    for (atom <- cq.getAtoms) {
      val atomVariables = ImmutableSet.copyOf(atom.getVariables)
      boundVariableUFTree.unionAll(SetLikeExtensions.intersection(
        atomVariables,
        cqBoundVariables
      ))
    }

    val boundVariableConnectedComponents = boundVariableUFTree.getEquivalenceClasses

    ImmutableSet.copyOf(boundVariableConnectedComponents.stream.map((component) =>
      ConjunctiveQueryExtensions.strictlyInduceSubqueryByVariables(
        cq,
        component
      ).get
      // this .get() call succeeds because
      // we are strictly inducing a subquery by
      // a maximally connected component of bound variables
    ).iterator)
  }
}
