package io.github.kory33.guardedqueries.core.formalinstance;

import com.google.common.collect.ImmutableList;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.Term;

import java.util.function.Function;

public record FormalFact<TermAlphabet>(Predicate predicate, ImmutableList<? extends TermAlphabet> appliedTerms) {
    @SafeVarargs
    public FormalFact {
    }

    public <T> FormalFact<T> map(final Function<? super TermAlphabet, ? extends T> mapper) {
        return new FormalFact<>(
                this.predicate,
                ImmutableList.copyOf(this.appliedTerms.stream().map(mapper).iterator())
        );
    }

    public static Atom asAtom(final FormalFact<Term> fact) {
        return Atom.create(fact.predicate, fact.appliedTerms.toArray(Term[]::new));
    }
}
