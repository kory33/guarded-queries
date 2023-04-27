package io.github.kory33.guardedqueries.core.subsumption.formula;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;

import java.util.Arrays;
import java.util.HashSet;

/**
 * An implementation of {@link MaximallySubsumingTGDSet} that keeps track of
 * a set of datalog rules which are "maximal" with respect to the following subsumption relation:
 * A rule R1 subsumes a rule R2 (according to this implementation) if
 * <ul>
 *     <li>the body of R1 is a subset of the body of R2</li>
 *     <li>the head of R1 is equal to the head of R2</li>
 * </ul>
 */
public class MinimalExactBodyDatalogRuleSet implements MaximallySubsumingTGDSet<DatalogRule> {
    private final HashSet<DatalogRule> rulesKnownToBeMaximalSoFar = new HashSet<>();

    /**
     * See if we can conclude that the first rule subsumes the second rule,
     * according to the subsumption relation defined in this class.
     */
    private static boolean firstRuleSubsumesSecond(DatalogRule first, DatalogRule second) {
        if (!Arrays.equals(first.getHeadAtoms(), second.getHeadAtoms())) {
            return false;
        }

        // we check that the body of the first rule is a subset of the body of the second rule
        subsetCheck:
        for (final var firstBodyAtom : first.getBodyAtoms()) {
            for (final var secondBodyAtom : second.getBodyAtoms()) {
                if (firstBodyAtom.equals(secondBodyAtom)) {
                    continue subsetCheck;
                }
            }
            return false;
        }
        return true;
    }

    private boolean isSubsumedByExistingRule(DatalogRule rule) {
        return rulesKnownToBeMaximalSoFar.stream()
                .anyMatch(existingRule -> firstRuleSubsumesSecond(existingRule, rule));
    }

    @Override
    public void addRule(DatalogRule rule) {
        if (!isSubsumedByExistingRule(rule)) {
            rulesKnownToBeMaximalSoFar.removeIf(existingRule -> firstRuleSubsumesSecond(rule, existingRule));
            rulesKnownToBeMaximalSoFar.add(rule);
        }
    }

    @Override
    public ImmutableSet<DatalogRule> getRules() {
        return ImmutableSet.copyOf(rulesKnownToBeMaximalSoFar);
    }
}
