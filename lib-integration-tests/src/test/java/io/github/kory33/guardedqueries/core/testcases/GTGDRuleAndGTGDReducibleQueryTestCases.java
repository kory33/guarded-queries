package io.github.kory33.guardedqueries.core.testcases;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.testcases.parser.FormulaParsers;
import io.github.kory33.guardedqueries.core.testcases.parser.FormulaParsingContext;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.TypedConstant;

/**
 * A static class containing test cases of GTGD rules and queries.
 */
public class GTGDRuleAndGTGDReducibleQueryTestCases {
    private GTGDRuleAndGTGDReducibleQueryTestCases() {
    }

    public static final FormulaParsingContext commonParsingContext = new FormulaParsingContext(
            // we regard all symbols of the form "c_{number}" as constants
            s -> s.startsWith("c_") && s.length() > 2 && s.substring(2).chars().allMatch(Character::isDigit),
            TypedConstant::create
    );
    private static final FormulaParsers.WhitespaceIgnoringParser<ConjunctiveQuery> conjunctiveQuery =
            new FormulaParsers.WhitespaceIgnoringParser<>(FormulaParsers.conjunctiveQueryParser(commonParsingContext));
    private static final FormulaParsers.WhitespaceIgnoringParser<GTGD> gtgd =
            new FormulaParsers.WhitespaceIgnoringParser<>(FormulaParsers.gtgdParser(commonParsingContext));

    // A rule set together with queries, adapted from
    // https://github.com/KRR-Oxford/Guarded-saturation/blob/bde32223ae4bc8ce084d233e7eede5ed1021adc7/src/test/java/uk/ac/ox/cs/gsat/SimpleSatTest.java#L49-L51
    public static class SimpleArity2Rule_0 {
        public static final ImmutableList<GTGD> rule = ImmutableList.of(
                gtgd.parse("A(x_1) -> EE x_2. R(x_1, x_2)"),
                gtgd.parse("R(x_1, x_2) -> U(x_2)"),
                gtgd.parse("R(x_1, x_2), U(x_2) -> P(x_1)")
        );

        public static final GTGDRuleAndGTGDReducibleQuery atomicQuery = new GTGDRuleAndGTGDReducibleQuery(
                rule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("R(z_1, z_1)"),
                        ImmutableList.of(),
                        conjunctiveQuery.parse("R(z_1, z_1)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery joinQuery = new GTGDRuleAndGTGDReducibleQuery(
                rule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)"),
                        ImmutableList.of(),
                        conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery existentialGuardedQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                rule,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y. R(x, y), R(y, y)"),
                        ImmutableList.of(
                                gtgd.parse("R(x, y), R(y, y) -> Goal(x)")
                        ),
                        conjunctiveQuery.parse("Goal(x)")
                )
        );

        public static final GTGDRuleAndGTGDReducibleQuery existentialJoinQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                rule,
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
                rule,
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

    // A rule set together with queries, adapted from
    // https://github.com/KRR-Oxford/Guarded-saturation/blob/bde32223ae4bc8ce084d233e7eede5ed1021adc7/src/test/java/uk/ac/ox/cs/gsat/SimpleSatTest.java#L81-L83
    public static class Arity4Rule {
        public static final ImmutableList<GTGD> higherArityRule_0 = ImmutableList.of(
                gtgd.parse("R(x_1, x_2), P(x_2) -> EE y_1, y_2. S(x_1, x_2, y_1, y_2), T(x_1, x_2, y_2)"),
                gtgd.parse("S(x_1, x_2, x_3, x_4) -> U(x_4)"),
                gtgd.parse("T(z_1, z_2, z_3), U(z_3) -> P(z_1)")
        );

        public static final GTGDRuleAndGTGDReducibleQuery atomicQuery = new GTGDRuleAndGTGDReducibleQuery(
                higherArityRule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("S(x, z, y, x)"),
                        ImmutableList.of(),
                        conjunctiveQuery.parse("S(x, z, y, x)")
                )
        );

        // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEComputation
        public static final GTGDRuleAndGTGDReducibleQuery existentialGuardedQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                higherArityRule_0,
                new GTGDReducibleConjunctiveQuery(
                        conjunctiveQuery.parse("EE y. S(x, y, y, z), R(y, x)"),
                        ImmutableList.of(
                                gtgd.parse("S(x, y, y, z), R(y, x) -> Goal(x, z)")
                        ),
                        conjunctiveQuery.parse("Goal(x, z)")
                )
        );

        // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEComputation
        public static final GTGDRuleAndGTGDReducibleQuery existentialJoinQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
                higherArityRule_0,
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
