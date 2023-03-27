package io.github.kory33.guardedqueries.core.subqueryentailments;

import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

import java.util.stream.Stream;

/**
 * An object of this class represents a computation of the
 * <code>SubqueryEntailmentInstances</code> problem.
 */
public final class SubqueryEntailmentComputation {
    private final SaturatedRuleSet<? extends NormalGTGD> saturatedRule;
    private final ConjunctiveQuery boundVariableConnectedSubquery;

    public SubqueryEntailmentComputation(
            final SaturatedRuleSet<? extends NormalGTGD> saturatedRule,
            final ConjunctiveQuery boundVariableConnectedSubquery
    ) {
        this.saturatedRule = saturatedRule;
        this.boundVariableConnectedSubquery = boundVariableConnectedSubquery;
    }

    /**
     * Enumerate all maximally-subsuming subquery entailments.
     */
    public Stream<SubqueryEntailmentInstance> run() {
        throw new RuntimeException("TODO: Unimplemented!");
    }
}
