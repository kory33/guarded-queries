package io.github.kory33.guardedqueries.core.testcases;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.testcases.parser.FormulaParsers;
import io.github.kory33.guardedqueries.core.testcases.parser.FormulaParsingContext;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.TypedConstant;

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

    // Adapted from
    // https://github.com/KRR-Oxford/Guarded-saturation/blob/bde32223ae4bc8ce084d233e7eede5ed1021adc7/src/test/java/uk/ac/ox/cs/gsat/SimpleSatTest.java#L49-L51
    public static final ImmutableList<GTGD> simpleRule_0 = ImmutableList.of(
            gtgd.parse("A(x_1) -> EE x_2. R(x_1, x_2)"),
            gtgd.parse("R(x_1, x_2) -> U(x_2)"),
            gtgd.parse("R(x_1, x_2), U(x_2) -> P(x_1)")
    );

    public static final GTGDRuleAndGTGDReducibleQuery simpleQuery_0_atomicQuery = new GTGDRuleAndGTGDReducibleQuery(
            simpleRule_0,
            new GTGDReducibleConjunctiveQuery(
                    conjunctiveQuery.parse("R(z_1, z_1)"),
                    ImmutableList.of(),
                    conjunctiveQuery.parse("R(z_1, z_1)")
            )
    );

    public static final GTGDRuleAndGTGDReducibleQuery simpleQuery_0_joinQuery = new GTGDRuleAndGTGDReducibleQuery(
            simpleRule_0,
            new GTGDReducibleConjunctiveQuery(
                    conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)"),
                    ImmutableList.of(),
                    conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)")
            )
    );

    public static final GTGDRuleAndGTGDReducibleQuery simpleQuery_0_existentialJoinQuery_0 = new GTGDRuleAndGTGDReducibleQuery(
            simpleRule_0,
            new GTGDReducibleConjunctiveQuery(
                    conjunctiveQuery.parse("EE y,z. R(w, y), R(y, z)"),
                    ImmutableList.of(
                            gtgd.parse("R(y, z) -> I(y)"),
                            gtgd.parse("R(w, y), I(y) -> Goal(w)")
                    ),
                    conjunctiveQuery.parse("Goal(w)")
            )
    );

    public static final GTGDRuleAndGTGDReducibleQuery simpleQuery_0_existentialJoinQuery_1 = new GTGDRuleAndGTGDReducibleQuery(
            simpleRule_0,
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
