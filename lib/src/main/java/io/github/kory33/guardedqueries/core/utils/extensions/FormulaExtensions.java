package io.github.kory33.guardedqueries.core.utils.extensions;

import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Formula;
import uk.ac.ox.cs.pdq.fol.Predicate;

import java.util.stream.Stream;

public class FormulaExtensions {
    private FormulaExtensions() {
    }

    public static Stream<Predicate> streamPredicatesAppearingIn(final Formula formula) {
        final var children = formula.getChildren();

        if (children.length == 0) {
            if (formula instanceof Atom) {
                return Stream.of(((Atom) formula).getPredicate());
            } else {
                return Stream.empty();
            }
        } else {
            return Stream.of(children).flatMap(FormulaExtensions::streamPredicatesAppearingIn);
        }
    }
}
