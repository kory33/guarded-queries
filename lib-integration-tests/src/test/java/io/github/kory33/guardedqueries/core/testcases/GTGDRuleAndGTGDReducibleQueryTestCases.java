package io.github.kory33.guardedqueries.core.testcases;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.parser.FormulaParsers;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

/**
 * A static class containing test cases of GTGD rules and GTGD-reducible queries.
 */
public class GTGDRuleAndGTGDReducibleQueryTestCases {
    private GTGDRuleAndGTGDReducibleQueryTestCases() {
    }

    private static final FormulaParsers.WhitespaceIgnoringParser<ConjunctiveQuery> conjunctiveQuery = TestCaseParsers.conjunctiveQuery;
    private static final FormulaParsers.WhitespaceIgnoringParser<GTGD> gtgd = TestCaseParsers.gtgd;

    public static class SimpleArity2Rule_0 {
        public static final GTGDRuleAndGTGDReducibleQuery atomicQuery = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("R(z_1, z_1)"),
                        ImmutableList.of(),
                        conjunctiveQuery.parse("R(z_1, z_1)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery joinQuery = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)"),
                        ImmutableList.of(),
                        conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery existentialGuardedQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y. R(x, y), R(y, y)"),
                        ImmutableList.of(
                                gtgd.parse("R(x, y), R(y, y) -> Goal(x)")
                        ),
                        conjunctiveQuery.parse("Goal(x)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery existentialJoinQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y,z. R(w, y), R(y, z)"),
                        ImmutableList.of(
                                gtgd.parse("R(y, z) -> I(y)"),
                                gtgd.parse("R(w, y), I(y) -> Goal(w)")
                        ),
                        conjunctiveQuery.parse("Goal(w)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery existentialJoinQuery_1 = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.simpleArity2Rule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y,z. R(y, t), R(t, w), R(w, z), U(z)"),
                        ImmutableList.of(
                                gtgd.parse("R(y, t) -> I_1(t)"),
                                gtgd.parse("R(w, z), U(z) -> I_2(w)")
                        ),
                        conjunctiveQuery.parse("I_1(t), R(t, w), I_2(w)")
                )
        );
    }

    public static class ConstantRule {
        public static final GTGDRuleAndGTGDReducibleQuery atomicQuery = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.constantRule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("R(c_3, x)"),
                        ImmutableList.of(),
                        conjunctiveQuery.parse("R(c_3, x)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery existentialBooleanQueryWithConstant = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.constantRule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y. R(c_3, y)"),
                        ImmutableList.of(
                                gtgd.parse("R(c_3, y) -> Goal()")
                        ),
                        conjunctiveQuery.parse("Goal()")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery existentialGuardedWithConstant = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.constantRule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y. R(c_1, y), R(y, w), R(w, c_3)"),
                        ImmutableList.of(
                                gtgd.parse("R(c_1, y), R(y, w), R(w, c_3) -> Goal(w)")
                        ),
                        conjunctiveQuery.parse("Goal(w)")
                )
        );
    }

    public static class Arity3Rule_0 {
        // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEComputation / NormalizingDPTableSEComputation
        public static final GTGDRuleAndGTGDReducibleQuery existentialJoinQuery = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.arity3Rule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y. T(x, y, z), T(y, x, z)"),
                        ImmutableList.of(
                                gtgd.parse("T(x, y, z), T(y, x, z) -> Goal(x, z)")
                        ),
                        conjunctiveQuery.parse("Goal(x, z)")
                )
        );
    }

    public static class Arity4Rule {
        public static final GTGDRuleAndGTGDReducibleQuery atomicQuery = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.arity4Rule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("S(x, z, y, x)"),
                        ImmutableList.of(),
                        conjunctiveQuery.parse("S(x, z, y, x)")
                )
        );

        // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEComputation / NormalizingDPTableSEComputation
        public static final GTGDRuleAndGTGDReducibleQuery existentialGuardedQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.arity4Rule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y. S(x, y, y, z), R(y, x)"),
                        ImmutableList.of(
                                gtgd.parse("S(x, y, y, z), R(y, x) -> Goal(x, z)")
                        ),
                        conjunctiveQuery.parse("Goal(x, z)")
                )
        );

        // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEComputation / NormalizingDPTableSEComputation
        public static final GTGDRuleAndGTGDReducibleQuery existentialJoinQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                GTGDRuleSets.arity4Rule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y_1, y_2. T(z_1, y_1, z_2), R(y_1, y_2), T(y_1, y_2, z_2)"),
                        ImmutableList.of(
                                gtgd.parse("R(y_1, y_2), T(y_1, y_2, z_2) -> I(y_1, z_2)"),
                                gtgd.parse("T(z_1, y_1, z_2), I(y_1, z_2) -> Goal(z_1, z_2)")
                        ),
                        conjunctiveQuery.parse("Goal(z_1, z_2)")
                )
        );
    }
}
