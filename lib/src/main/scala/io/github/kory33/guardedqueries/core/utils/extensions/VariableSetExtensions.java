package io.github.kory33.guardedqueries.core.utils.extensions;

import com.google.common.collect.ImmutableList;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;

public class VariableSetExtensions {
    private VariableSetExtensions() {
    }

    /**
     * Deduplicate and sort the given collection of variables by their lexicographical order.
     */
    public static ImmutableList<Variable> sortBySymbol(
            final Collection<? extends Variable> variables
    ) {
        final var sortedVariables = new ArrayList<>(new HashSet<>(variables));
        sortedVariables.sort(Comparator.comparing((Variable v) -> v.getSymbol()));
        return ImmutableList.copyOf(sortedVariables);
    }
}
