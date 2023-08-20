package io.github.kory33.guardedqueries.core.subsumption.formula;

import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.pdq.fol.TGD;

import java.util.HashSet;

/**
 * A partial implementation of {@link MaximallySubsumingTGDSet} that does not use any index
 * to filter out subsumption candidates.
 */
public abstract class IndexlessMaximallySubsumingTGDSet<F extends TGD> implements MaximallySubsumingTGDSet<F> {
    /**
     * Judge if the first rule subsumes the second rule,
     * according to an implementation-specific subsumption relation.
     */
    protected abstract boolean firstRuleSubsumesSecond(F first, F second);

    private final HashSet<F> rulesKnownToBeMaximalSoFar = new HashSet<>();

    private boolean isSubsumedByExistingRule(F rule) {
        return rulesKnownToBeMaximalSoFar.stream()
                .anyMatch(existingRule -> firstRuleSubsumesSecond(existingRule, rule));
    }

    @Override
    public final void addRule(F rule) {
        if (!isSubsumedByExistingRule(rule)) {
            rulesKnownToBeMaximalSoFar.removeIf(existingRule -> firstRuleSubsumesSecond(rule, existingRule));
            rulesKnownToBeMaximalSoFar.add(rule);
        }
    }

    @Override
    public final ImmutableSet<F> getRules() {
        return ImmutableSet.copyOf(rulesKnownToBeMaximalSoFar);
    }
}
