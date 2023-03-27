package io.github.kory33.guardedqueries.core.utils.extensions;

import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Term;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.Map;

public class AtomExtensions {
    private AtomExtensions() {
    }

    public static Atom substitute(final Atom atom, final Map<Variable, Term> substitution) {
        final var newTerms = Arrays.stream(atom.getTerms())
                .map(term -> {
                    if (term instanceof Variable) {
                        return substitution.getOrDefault((Variable) term, term);
                    } else {
                        return term;
                    }
                })
                .toArray(Term[]::new);
        return Atom.create(atom.getPredicate(), newTerms);
    }
}
