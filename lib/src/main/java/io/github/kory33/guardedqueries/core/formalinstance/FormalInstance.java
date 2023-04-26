package io.github.kory33.guardedqueries.core.formalinstance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Term;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public class FormalInstance<TermAlphabet> {
    public final ImmutableSet<FormalFact<TermAlphabet>> facts;
    private ImmutableSet<TermAlphabet> activeTerms;

    public FormalInstance(final Iterator<FormalFact<TermAlphabet>> fact) {
        this.facts = ImmutableSet.copyOf(fact);
    }

    public FormalInstance(final Collection<FormalFact<TermAlphabet>> facts) {
        this.facts = ImmutableSet.copyOf(facts);
    }

    public FormalInstance(final ImmutableSet<FormalFact<TermAlphabet>> facts) {
        this.facts = facts;
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
        return ImmutableSet.copyOf(StreamExtensions.filterSubtype(this.getActiveTerms().stream(), clazz).iterator());
    }

    public <T> FormalInstance<T> map(final Function<TermAlphabet, T> mapper) {
        return FormalInstance.fromIterator(this.facts.stream().map(fact -> fact.map(mapper)).iterator());
    }

    public FormalInstance<TermAlphabet> restrictToAlphabetsWith(final Predicate<TermAlphabet> predicate) {
        return fromIterator(
                this.facts.stream()
                        .filter(fact -> fact.appliedTerms().stream().allMatch(predicate))
                        .iterator()
        );
    }

    public FormalInstance<TermAlphabet> restrictToSignature(final FunctionFreeSignature signature) {
        return fromIterator(
                this.facts.stream()
                        .filter(fact -> signature.predicates().contains(fact.predicate()))
                        .iterator()
        );
    }

    public boolean containsFact(final FormalFact<TermAlphabet> fact) {
        return this.facts.contains(fact);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FormalInstance<?> that)) return false;
        return Objects.equals(facts, that.facts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(facts);
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

    public static <TermAlphabet> FormalInstance<TermAlphabet> unionAll(final Iterable<FormalInstance<TermAlphabet>> instances) {
        final var factSetBuilder = ImmutableSet.<FormalFact<TermAlphabet>>builder();
        instances.forEach(instance -> factSetBuilder.addAll(instance.facts));
        return new FormalInstance<>(factSetBuilder.build());
    }

    public static <TermAlphabet> FormalInstance<TermAlphabet> empty() {
        return new FormalInstance<>(ImmutableSet.of());
    }

    @SafeVarargs
    public static <TermAlphabet> FormalInstance<TermAlphabet> of(final FormalFact<TermAlphabet>... facts) {
        return new FormalInstance<>(ImmutableSet.copyOf(facts));
    }

    @Override
    public String toString() {
        return facts.toString();
    }
}
