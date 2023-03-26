package io.github.kory33.guardedqueries.core.fol;

import uk.ac.ox.cs.pdq.fol.*;

/**
 * A class of Datalog rules.
 * <p>
 * A Datalog rule is a Dependency such that
 * <ul>
 *   <li>all variables are universally quantified, and</li>
 *   <li>every variable in the head appears in some atom in the body.</li>
 * </ul>
 */
public class DatalogRule extends TGD {
    public DatalogRule(Atom[] body, Atom[] head) {
        super(body, head);
        if (existential.length != 0) {
            throw new IllegalArgumentException(
                    "Datalog rule cannot contain existential variables, got " + super.toString()
            );
        }
    }

    private static boolean notConjunctionOfAtoms(final Formula formula) {
        if (formula instanceof Atom) {
            return false;
        }

        if (!(formula instanceof Conjunction)) {
            return true;
        }

        for (final var childFormula : ((Conjunction) formula).getChildren()) {
            if (notConjunctionOfAtoms(childFormula)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Attempt to regard a given dependency as a Datalog rule.
     * <p>
     * This method may fail with an <code>IllegalArgumentException</code>
     * if the given dependency has existential variables in the head, or
     * if proper subformulae are not conjunctions of atoms.
     */
    public static DatalogRule tryFromDependency(final Dependency dependency) {
        final var body = dependency.getBody();
        final var head = dependency.getHead();

        if (notConjunctionOfAtoms(body)) {
            throw new IllegalArgumentException("Body of a DatalogRule must be a conjunction of atoms, got" + body.toString());
        }

        if (notConjunctionOfAtoms(head)) {
            throw new IllegalArgumentException("Head of a DatalogRule must be a conjunction of atoms, got" + head.toString());
        }

        return new DatalogRule(dependency.getBodyAtoms(), dependency.getHeadAtoms());
    }
}
