package io.github.kory33.guardedqueries.core.fol;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.FormulaExtra;
import uk.ac.ox.cs.pdq.fol.Formula;
import uk.ac.ox.cs.pdq.fol.Predicate;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * An object of this class represents a first-order logic signature with
 * - countably-infinitely many constants
 * - finite set of predicate symbols
 * - no function symbols
 */
public record FunctionFreeSignature(ImmutableSet<Predicate> predicates) {
    public FunctionFreeSignature(Iterable<? extends Predicate> predicates) {
        this(ImmutableSet.copyOf(predicates));
    }

    public static FunctionFreeSignature fromFormulas(final Collection<Formula> formulas) {
        return new FunctionFreeSignature(
                formulas
                        .stream()
                        .flatMap(FormulaExtra::streamPredicatesAppearingIn)
                        .collect(Collectors.toList())
        );
    }
}
