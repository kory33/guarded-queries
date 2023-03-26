package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree;
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

import java.util.Arrays;

/**
 * A class of objects representing the decomposition of a conjunctive query into
 * bound-variable-connected components. The decomposition consists of bound-variable-free atoms
 * and maximally bound-variable-connected subqueries.
 * <p>
 * For example, given a conjunctive query {@code ∃x,y,z. U(c) ∧ U(w) ∧ U(x) ∧ R(x,w) ∧ T(z,c,w) ∧ R(y,z)}
 * with a free variable {@code w} and a constant {@code c},
 * <ul>
 *   <li>the set of bound-variable-free atoms is { U(c), U(w) }, and </li>
 *   <li>
 *     the set of maximally (bound-variable-) connected subqueries is
 *     {@code { ∃x. U(x) ∧ R(x,w), ∃y,z. T(z,c,w) ∧ R(y,z) }}.
 *   </li>
 * </ul>
 * Note that the atom {@code R(x,w)} and the atom {@code T(z,c,w)} are not bound-variable-connected
 * (therefore separated into different subqueries) because the variable {@code w} is free in the CQ.
 * <p>
 * The constructor takes a {@link ConjunctiveQuery} object and computes the decomposition.
 */
public class CQBoundVariableConnectedComponents {
    public final ImmutableSet<Atom> boundVariableFreeAtoms;
    public final ImmutableSet<ConjunctiveQuery> maximallyConnectedSubqueries;

    public CQBoundVariableConnectedComponents(final ConjunctiveQuery cq) {
        final var cqBoundVariables = ImmutableSet.copyOf(cq.getBoundVariables());
        this.boundVariableFreeAtoms = ImmutableSet.copyOf(
                Arrays.stream(cq.getAtoms())
                        .filter(atom -> Arrays.stream(atom.getVariables()).anyMatch(cqBoundVariables::contains))
                        .iterator()
        );

        // split bound variables into connected components
        final var boundVariableUFTree = new SimpleUnionFindTree<>(cqBoundVariables);
        for (final var atom : cq.getAtoms()) {
            final var atomVariables = ImmutableSet.copyOf(atom.getVariables());
            boundVariableUFTree.unionAll(SetExtensions.intersection(atomVariables, cqBoundVariables));
        }
        final var boundVariableConnectedComponents = boundVariableUFTree.getEquivalenceClasses();

        this.maximallyConnectedSubqueries = ImmutableSet.copyOf(
                boundVariableConnectedComponents
                        .stream()
                        .map(component -> ConjunctiveQueryExtensions.strictlyInduceSubqueryByVariables(cq, component))
                        .iterator()
        );
    }
}
