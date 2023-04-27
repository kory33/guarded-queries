package io.github.kory33.guardedqueries.core.subsumption.formula;

import com.google.common.collect.ImmutableSet;
import uk.ac.ox.cs.pdq.fol.TGD;

/**
 * An interface that can keep track of a set of datalog rules
 * which are "maximal" with respect to a certain subsumption relation.
 * <p>
 * An object conforming to this interface is typically initialized to the "empty" state,
 * and then multiple Datalog rules are then added via {@link #addRule} method.
 * Finally, the set of rules can be retrieved via {@link #getRules} method.
 */
public interface MaximallySubsumingTGDSet<F extends TGD> {
    /**
     * Add a rule to the set.
     * <p>
     * The method should check that the rule is not subsumed by any of the rules already in the set,
     * and if it is not, all rules that are subsumed by the new rule should be removed
     * before the new rule is added to the set.
     */
    void addRule(F rule);

    /**
     * Get the set of rules currently recorded in the set.
     */
    ImmutableSet<F> getRules();

    @FunctionalInterface
    interface Factory<F extends TGD, S extends MaximallySubsumingTGDSet<F>> {
        S emptyRuleSet();
    }
}
