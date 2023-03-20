package io.github.kory33.guardedqueries.core.subqueryentailments;

import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

import java.util.Collection;

/**
 * An object of this class represents a computation of the
 * <code>SubqueryEntailmentInstances</code> problem.
 */
public class SubqueryEntailmentComputation {
    private final Collection<? extends GTGD> rules;
    private final Collection<? extends GTGD> saturatedRule;
    private final ConjunctiveQuery query;

    private Collection<SubqueryEntailmentInstance> resultCache = null;

    /**
     * @param saturatedRule a saturation of <code>rules</code>. The behaviour of this object is
     *                      undefined if <code>saturatedRule</code> is not a saturation of <code>rules</code>.
     */
    public SubqueryEntailmentComputation(
            final Collection<? extends GTGD> rules,
            final Collection<? extends GTGD> saturatedRule,
            final ConjunctiveQuery query
    ) {
        this.rules = rules;
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
