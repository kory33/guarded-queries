package io.github.kory33.guardedqueries.core.testcases;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Collection;

/**
 * A test case containing GTGD rules and a conjunctive query.
 */
public record GTGDRuleAndConjunctiveQuery(
        Collection<? extends GTGD> guardedRules,
        ConjunctiveQuery query
) {
    public FunctionFreeSignature signature() {
        return FunctionFreeSignature.encompassingRuleQuery(this.guardedRules, query);
    }

    public ImmutableList<Variable> deduplicatedQueryFreeVariables() {
        return ImmutableList.copyOf(ImmutableSet.copyOf(query.getFreeVariables()));
    }
}
