package io.github.kory33.guardedqueries.core.datalog;

import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import uk.ac.ox.cs.pdq.fol.Dependency;

import java.util.Collection;

public record DatalogProgram(Collection<DatalogRule> rules) {
    public static DatalogProgram tryFromDependencies(final Collection<Dependency> dependencies) {
        return new DatalogProgram(dependencies.stream().map(DatalogRule::tryFromDependency).toList());
    }
}
