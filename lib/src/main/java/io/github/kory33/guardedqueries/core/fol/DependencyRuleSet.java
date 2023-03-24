package io.github.kory33.guardedqueries.core.fol;

import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Dependency;
import uk.ac.ox.cs.pdq.fol.Formula;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

public class DependencyRuleSet<RuleClass extends Dependency> {
    public final ImmutableSet<RuleClass> rules;

    private ImmutableSet<Constant> headConstantsCache;
    private ImmutableSet<Constant> bodyConstantsCache;
    private ImmutableSet<Constant> constantsCache;

    private static Stream<Constant> constantsInFormula(final Formula formula) {
        return Arrays.stream(formula.getTerms())
                .flatMap(term -> term instanceof Constant
                        ? Stream.of((Constant) term)
                        : Stream.empty()
                );
    }

    public DependencyRuleSet(final Collection<? extends RuleClass> rules) {
        this.rules = ImmutableSet.copyOf(rules);
    }

    public ImmutableSet<Constant> getHeadConstants() {
        if (this.headConstantsCache == null) {
            this.headConstantsCache = ImmutableSet.copyOf(
                    this.rules.stream()
                            .map(Dependency::getHead)
                            .flatMap(DependencyRuleSet::constantsInFormula)
                            .iterator()
            );
        }

        return this.headConstantsCache;
    }

    public ImmutableSet<Constant> getBodyConstants() {
        if (this.bodyConstantsCache == null) {
            this.bodyConstantsCache = ImmutableSet.copyOf(
                    this.rules.stream()
                            .map(Dependency::getBody)
                            .flatMap(DependencyRuleSet::constantsInFormula)
                            .iterator()
            );
        }

        return this.bodyConstantsCache;
    }

    public ImmutableSet<Constant> getConstants() {
        if (this.constantsCache == null) {
            this.constantsCache = ImmutableSet
                    .<Constant>builder()
                    .addAll(this.getHeadConstants())
                    .addAll(this.getBodyConstants())
                    .build();
        }

        return this.constantsCache;
    }
}
