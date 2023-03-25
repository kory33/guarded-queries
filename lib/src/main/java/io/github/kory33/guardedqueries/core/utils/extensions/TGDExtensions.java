package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.pdq.fol.TGD;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TGDExtensions {
    private TGDExtensions() {
    }

    /**
     * Compute the frontier of a given TGD, that is, the set of variables
     * that appear in both the body and the head of the TGD.
     */
    public static ImmutableSet<Variable> frontierVariables(final TGD tgd) {
        final var headVariables = tgd.getHead().getBoundVariables();
        final var bodyVariables = tgd.getBody().getBoundVariables();

        final var headVariablesMutableSet = new HashSet<>(Arrays.asList(headVariables));
        headVariablesMutableSet.retainAll(Set.of(bodyVariables));
        return ImmutableSet.copyOf(headVariablesMutableSet);
    }
}
