package io.github.kory33.guardedqueries.core.rewriting;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.Constant;
import uk.ac.ox.cs.pdq.fol.Formula;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

public class SaturatedRuleSet<RuleClass extends GTGD> {
    public final ImmutableList<GTGD> saturatedRules;
    public final DatalogProgram saturatedRulesAsDatalogProgram;

    public final ImmutableList<RuleClass> existentialRules;
    public final ImmutableList<GTGD> allRules;

    private ImmutableSet<Constant> constantsCache;

    public SaturatedRuleSet(
            final AbstractSaturation<? extends GTGD> saturation,
            final Collection<? extends RuleClass> originalRules
    ) {
        this.saturatedRules = ImmutableList.copyOf(saturation.run(new ArrayList<>(originalRules)));
        this.saturatedRulesAsDatalogProgram = DatalogProgram.tryFromDependencies(this.saturatedRules);
        this.existentialRules = ImmutableList.copyOf(
                originalRules.stream()
                        .filter(rule -> rule.getExistential().length > 0)
                        .iterator()
        );

        final var allRulesBuilder = ImmutableList.<GTGD>builder();
        allRulesBuilder.addAll(existentialRules);
        allRulesBuilder.addAll(saturatedRules);
        this.allRules = allRulesBuilder.build();
    }

    private static Stream<Constant> constantsInFormula(final Formula formula) {
        return StreamExtensions.filterSubtype(Arrays.stream(formula.getTerms()), Constant.class);
    }

    public ImmutableSet<Constant> constants() {
        if (this.constantsCache == null) {
            this.constantsCache = ImmutableSet.copyOf(
                    this.allRules.stream()
                            .flatMap(SaturatedRuleSet::constantsInFormula)
                            .iterator()
            );
        }

        return this.constantsCache;
    }
}
