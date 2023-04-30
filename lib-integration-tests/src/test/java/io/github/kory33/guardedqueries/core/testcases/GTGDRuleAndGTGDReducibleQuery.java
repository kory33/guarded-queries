package io.github.kory33.guardedqueries.core.testcases;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Collection;

/**
 * A test case containing GTGD rules and a GTGD-reducible query.
 */
public record GTGDRuleAndGTGDReducibleQuery(
        Collection<? extends GTGD> guardedRules,
        GTGDReducibleConjunctiveQuery reducibleQuery
) {
    public FunctionFreeSignature signatureOfOriginalQuery() {
        return FunctionFreeSignature.encompassingRuleQuery(
                this.guardedRules,
                this.reducibleQuery.originalQuery()
        );
    }

    public GTGDRuleAndConjunctiveQuery asGTGDRuleAndConjunctiveQuery() {
        return new GTGDRuleAndConjunctiveQuery(this.guardedRules, this.reducibleQuery.originalQuery());
    }

    public ImmutableList<Variable> deduplicatedQueryFreeVariables() {
        return asGTGDRuleAndConjunctiveQuery().deduplicatedQueryFreeVariables();
    }
}
