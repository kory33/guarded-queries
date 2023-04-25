package io.github.kory33.guardedqueries.core.testcases;

import uk.ac.ox.cs.gsat.GTGD;

import java.util.Collection;

public record GTGDRuleAndGTGDReducibleQuery(
        Collection<? extends GTGD> guardedRules,
        GTGDReducibleConjunctiveQuery reducibleQuery
) {
}
