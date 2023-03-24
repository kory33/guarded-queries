package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.fol.DependencyRuleSet;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;

import java.util.ArrayList;
import java.util.Collection;

public class SaturatedRuleSet<RuleClass extends GTGD> {
    public final DependencyRuleSet<RuleClass> rules;
    public final ImmutableSet<GTGD> saturatedRules;

    public SaturatedRuleSet(
            final AbstractSaturation<? extends GTGD> saturation,
            final Collection<? extends RuleClass> normalRules
    ) {
        this.rules = new DependencyRuleSet<>(normalRules);
        this.saturatedRules = ImmutableSet.copyOf(saturation.run(new ArrayList<>(normalRules)));
    }
}
