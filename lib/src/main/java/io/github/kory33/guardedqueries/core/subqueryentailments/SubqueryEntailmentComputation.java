package io.github.kory33.guardedqueries.core.subqueryentailments;

import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

import java.util.Collection;

/**
 * An object of this class represents a computation of the
 * <code>SubqueryEntailmentInstances</code> problem.
 */
public final class SubqueryEntailmentComputation {
    final SaturatedRuleSet<NormalGTGD> saturatedRule;
    private final ConjunctiveQuery query;

    private Collection<SubqueryEntailmentInstance> resultCache = null;

    public SubqueryEntailmentComputation(
            final SaturatedRuleSet<NormalGTGD> saturatedRule,
            final ConjunctiveQuery query
    ) {
        this.saturatedRule = saturatedRule;
        this.query = query;
    }

    /**
     * Enumerate all maximally-subsuming subquery entailments.
     */
    public Collection<SubqueryEntailmentInstance> run() {
        if (resultCache != null) {
            return resultCache;
        }

        throw new RuntimeException("TODO: Unimplemented!");
    }
}
