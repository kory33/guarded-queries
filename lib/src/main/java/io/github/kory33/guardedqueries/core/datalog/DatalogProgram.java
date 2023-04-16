package io.github.kory33.guardedqueries.core.datalog;

import com.google.common.collect.ImmutableList;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import uk.ac.ox.cs.pdq.fol.Dependency;

import java.util.Collection;

public record DatalogProgram(Collection<DatalogRule> rules) {
    public static DatalogProgram tryFromDependencies(final Collection<? extends Dependency> dependencies) {
        return new DatalogProgram(dependencies.stream().map(DatalogRule::tryFromDependency).toList());
    }

    @Override
    public String toString() {
        return ImmutableList.copyOf(rules).toString();
    }
}
