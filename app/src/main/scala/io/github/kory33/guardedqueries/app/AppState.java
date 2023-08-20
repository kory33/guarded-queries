package io.github.kory33.guardedqueries.app;

import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.Dependency;

public record AppState(ImmutableSet<GTGD> registeredRules) {
    public AppState() {
        this(ImmutableSet.of());
    }

    public AppState registerRule(GTGD rule) {
        return new AppState(ImmutableSet.<GTGD>builder().addAll(this.registeredRules).add(rule).build());
    }

    public ImmutableSet<Dependency> registeredRulesAsDependencies() {
        return ImmutableSet.copyOf(
                this.registeredRules.stream().<Dependency>map(r -> r).iterator()
        );
    }
}
