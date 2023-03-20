package io.github.kory33.guardedqueries.core.formalinstance;

import uk.ac.ox.cs.pdq.fol.Predicate;

public record FormalFact<TermAlphabet>(Predicate predicate, TermAlphabet... appliedTerms) {
    @SafeVarargs
    public FormalFact {
    }
}
