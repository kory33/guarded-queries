package io.github.kory33.guardedqueries.core.fol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.extensions.FormulaExtensions;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
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

    public static FunctionFreeSignature fromFormulas(final Collection<? extends Formula> formulas) {
        return new FunctionFreeSignature(
                formulas
                        .stream()
                        .flatMap(FormulaExtensions::streamPredicatesAppearingIn)
                        .collect(Collectors.toList())
        );
    }

    public static FunctionFreeSignature encompassingRuleQuery(
            final Collection<? extends GTGD> rules,
            final ConjunctiveQuery query
    ) {
        return FunctionFreeSignature.fromFormulas(
                ImmutableList
                        .<Formula>builder()
                        .addAll(rules)
                        .add(query)
                        .build()
        );
    }

    public ImmutableSet<String> predicateNames() {
        return ImmutableSet.copyOf(predicates.stream().map(Predicate::getName).toList());
    }

    public int maxArity() {
        return predicates.stream()
                .mapToInt(Predicate::getArity)
                .max()
                .orElse(0);
    }
}
