package io.github.kory33.guardedqueries.core.subsumption.formula;

import io.github.kory33.guardedqueries.core.fol.DatalogRule;

import java.util.Arrays;

/**
 * An implementation of {@link IndexlessMaximallySubsumingTGDSet} that keeps track of
 * a set of datalog rules which are "maximal" with respect to the following subsumption relation:
 * A rule R1 subsumes a rule R2 (according to this implementation) if
 * <ul>
 *     <li>the body of R1 is a subset of the body of R2</li>
 *     <li>the head of R1 is equal to the head of R2</li>
 * </ul>
 */
public final class MinimalExactBodyDatalogRuleSet extends IndexlessMaximallySubsumingTGDSet<DatalogRule> {
    @Override
    protected boolean firstRuleSubsumesSecond(DatalogRule first, DatalogRule second) {
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
}
