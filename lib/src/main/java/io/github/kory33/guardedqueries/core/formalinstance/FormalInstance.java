package io.github.kory33.guardedqueries.core.formalinstance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Term;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

public class FormalInstance<TermAlphabet> {
    public final ImmutableSet<FormalFact<TermAlphabet>> facts;
    private ImmutableSet<TermAlphabet> activeTerms;

    public FormalInstance(final Collection<FormalFact<TermAlphabet>> facts) {
        this.facts = ImmutableSet.copyOf(facts);
    }

    public ImmutableSet<TermAlphabet> getActiveTerms() {
        if (this.activeTerms == null) {
            this.activeTerms = ImmutableSet.copyOf(this.facts
                    .stream()
                    .flatMap(fact -> fact.appliedTerms().stream())
                    .iterator()
            );
        }
        return this.activeTerms;
    }

    public <T extends TermAlphabet> ImmutableSet<T> getActiveTermsInClass(final Class<T> clazz) {
        return ImmutableSet.copyOf(
                this.getActiveTerms().stream()
                        .filter(clazz::isInstance)
                        .map(clazz::cast)
                        .iterator()
        );
    }

    public <T> FormalInstance<T> map(final Function<TermAlphabet, T> mapper) {
        return FormalInstance.fromIterator(this.facts.stream().map(fact -> fact.map(mapper)).iterator());
    }

    public static ImmutableList<Atom> asAtoms(final FormalInstance<Term> instance) {
        return ImmutableList.copyOf(
                instance.facts.stream()
                        .map(FormalFact::asAtom)
                        .iterator()
        );
    }

    public static <TermAlphabet> FormalInstance<TermAlphabet> fromIterator(final Iterator<FormalFact<TermAlphabet>> facts) {
        return new FormalInstance<>(ImmutableSet.copyOf(facts));
    }
}
