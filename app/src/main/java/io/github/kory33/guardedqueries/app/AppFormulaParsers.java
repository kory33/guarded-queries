package io.github.kory33.guardedqueries.app;

import io.github.kory33.guardedqueries.parser.FormulaParsers;
import io.github.kory33.guardedqueries.parser.FormulaParsingContext;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.TypedConstant;

public class AppFormulaParsers {
    public static final FormulaParsingContext context = new FormulaParsingContext(
            // we regard all symbols of the form "c_{number}" as constants
            s -> s.startsWith("c_") && s.length() > 2 && s.substring(2).chars().allMatch(Character::isDigit),
            TypedConstant::create
    );

    public static final FormulaParsers.WhitespaceIgnoringParser<ConjunctiveQuery> conjunctiveQuery =
            new FormulaParsers.WhitespaceIgnoringParser<>(FormulaParsers.conjunctiveQueryParser(context));

    public static final FormulaParsers.WhitespaceIgnoringParser<GTGD> gtgd =
            new FormulaParsers.WhitespaceIgnoringParser<>(FormulaParsers.gtgdParser(context));
}
