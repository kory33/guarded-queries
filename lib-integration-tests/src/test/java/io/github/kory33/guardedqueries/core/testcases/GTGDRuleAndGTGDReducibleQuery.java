package io.github.kory33.guardedqueries.core.testcases;

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import uk.ac.ox.cs.gsat.GTGD;

import java.util.Collection;

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
}
