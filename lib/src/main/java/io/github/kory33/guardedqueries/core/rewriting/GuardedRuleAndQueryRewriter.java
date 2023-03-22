package io.github.kory33.guardedqueries.core.rewriting;

import io.github.kory33.guardedqueries.core.datalog.DatalogProgram;
import io.github.kory33.guardedqueries.core.datalog.DatalogQuery;
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature;
import io.github.kory33.guardedqueries.core.fol.NormalGTGD;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentComputation;
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance;
import io.github.kory33.guardedqueries.core.utils.StreamExtra;
import uk.ac.ox.cs.gsat.AbstractSaturation;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;
import uk.ac.ox.cs.pdq.fol.Dependency;
import uk.ac.ox.cs.pdq.fol.Predicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

public record GuardedRuleAndQueryRewriter(AbstractSaturation<? extends GTGD> saturation) {
    private Dependency subqueryEntailmentToSubgoalRule(final SubqueryEntailmentInstance subqueryEntailment) {
        throw new RuntimeException("TODO: not implemented!");
    }

    private Collection<Dependency> generateAllSubgoalGlueingRules() {
        throw new RuntimeException("TODO: not implemented!");
    }

    private Predicate pickGoalPredicate() {
        throw new RuntimeException("TODO: not implemented!");
    }

    /**
     * Compute the Datalog rewriting of a finite set of GTGD rules and a conjunctive query.
     */
    public DatalogQuery rewrite(final Collection<? extends GTGD> rules, final ConjunctiveQuery query) {
        final var normalizedRules = NormalGTGD.normalize(rules, FunctionFreeSignature.encompassingRuleQuery(rules, query));
        final var saturatedRules = saturation.run(new ArrayList<>(normalizedRules));

        final var goalPredicate = this.pickGoalPredicate();
        final var subgoalRules = new SubqueryEntailmentComputation(normalizedRules, saturatedRules, query)
                .run()
                .stream()
                .map(this::subqueryEntailmentToSubgoalRule);

        final var rewritingResult = StreamExtra.concatAll(
                saturatedRules.stream(),
                subgoalRules,
                this.generateAllSubgoalGlueingRules().stream()
        ).collect(Collectors.toList());

        return new DatalogQuery(
                DatalogProgram.tryFromDependencies(rewritingResult),
                goalPredicate
        );
    }
}
