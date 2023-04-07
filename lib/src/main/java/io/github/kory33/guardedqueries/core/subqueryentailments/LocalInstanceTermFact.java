package io.github.kory33.guardedqueries.core.subqueryentailments;

import io.github.kory33.guardedqueries.core.formalinstance.FormalFact;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.function.Function;

public class LocalInstanceTermFact {
    private LocalInstanceTermFact() {
    }

    public static FormalFact<LocalInstanceTerm> fromAtomWithVariableMap(
            final Atom fact,
            final Function<? super Variable, ? extends LocalInstanceTerm> mapper
    ) {
        return FormalFact.fromAtom(fact).map(term -> LocalInstanceTerm.fromTermWithVariableMap(term, mapper));
    }
}
