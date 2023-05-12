package io.github.kory33.guardedqueries.core.testcases;

import io.github.kory33.guardedqueries.parser.FormulaParsers;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

/**
 * A static class containing test cases of GTGD rules and conjunctive queries.
 */
public class GTGDRuleAndConjunctiveQueryTestCases {
    private GTGDRuleAndConjunctiveQueryTestCases() {
    }

    private static final FormulaParsers.WhitespaceIgnoringParser<ConjunctiveQuery> conjunctiveQuery = TestCaseParsers.conjunctiveQuery;

    public static class SimpleArity2Rule_0 {
        public static final GTGDRuleAndConjunctiveQuery nonReducibleJoinQuery = new GTGDRuleAndConjunctiveQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                conjunctiveQuery.parse("EE y. R(z, y), R(y, w)")
        );

        public static final GTGDRuleAndConjunctiveQuery triangleBCQ = new GTGDRuleAndConjunctiveQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                conjunctiveQuery.parse("EE x,y,z. R(x, y), R(y, z), R(z, x)")
        );

        public static final GTGDRuleAndConjunctiveQuery triangleBCQWithLeaf = new GTGDRuleAndConjunctiveQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                conjunctiveQuery.parse("EE x,y,z. R(x, y), R(y, z), R(z, x), R(x, w)")
        );
    }
}
