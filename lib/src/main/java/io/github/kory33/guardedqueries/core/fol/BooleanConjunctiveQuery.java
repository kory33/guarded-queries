package io.github.kory33.guardedqueries.core.fol;

import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Variable;

/**
 * A class of conjunctive queries that do not have free variables.
 */
public class BooleanConjunctiveQuery extends ConjunctiveQuery {
    public BooleanConjunctiveQuery(Atom[] children) {
        super(new Variable[0], children);
    }

    public static BooleanConjunctiveQuery tryFromCQ(final ConjunctiveQuery cq) {
        if (cq.getFreeVariables().length != 0) {
            throw new IllegalArgumentException("Expected CQ with no free variables, got " + cq.toString());
        }

        return new BooleanConjunctiveQuery(cq.getAtoms());
    }
}
