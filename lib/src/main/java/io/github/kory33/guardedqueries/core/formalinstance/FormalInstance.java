package io.github.kory33.guardedqueries.core.formalinstance;

import java.util.Collection;

public class FormalInstance<TermAlphabet> {
    protected final Collection<FormalFact<TermAlphabet>> facts;

    protected FormalInstance(final Collection<FormalFact<TermAlphabet>> facts) {
        this.facts = facts;
    }
}
