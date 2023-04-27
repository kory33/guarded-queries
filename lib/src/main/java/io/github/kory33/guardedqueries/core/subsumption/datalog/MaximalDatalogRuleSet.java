package io.github.kory33.guardedqueries.core.subsumption.datalog;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;

/**
 * An interface that can keep track of a set of datalog rules
 * which are "maximal" with respect to a certain subsumption relation.
 * <p>
 * An object conforming to this interface is typically initialized to the "empty" state,
 * and then multiple Datalog rules are then added via {@link #addRule} method.
 * Finally, the set of rules can be retrieved via {@link #getRules} method.
 */
public interface MaximalDatalogRuleSet {
    /**
     * Add a rule to the set.
     * <p>
     * The method should check that the rule is not subsumed by any of the rules already in the set,
     * and if it is not, all rules that are subsumed by the new rule should be removed
     * before the new rule is added to the set.
     */
    void addRule(DatalogRule rule);

    /**
     * Get the set of rules currently recorded in the set.
     */
    ImmutableSet<DatalogRule> getRules();

    @FunctionalInterface
    interface Factory<S extends MaximalDatalogRuleSet> {
        S emptyRuleSet();
    }
}
