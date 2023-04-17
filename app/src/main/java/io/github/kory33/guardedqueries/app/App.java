package io.github.kory33.guardedqueries.app;

import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subqueryentailments.computationimpls.NaiveDPTableSEComputation;
import uk.ac.ox.cs.gsat.GSat;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class App {
    private static String formatRule(final TGD rule) {
        return rule.getHead() + " :- " + rule.getBody();
    }

    private static final GuardedRuleAndQueryRewriter rewriter = new GuardedRuleAndQueryRewriter(
            GSat.getInstance(),
            new NaiveDPTableSEComputation(new NaiveSaturationEngine())
    );

    private static void runAndReportTime(final List<GTGD> rules, final ConjunctiveQuery query) {
        final var startTime = System.nanoTime();
        final var result = rewriter.rewrite(rules, query);
        final var endTime = System.nanoTime();

        System.out.println("================ Input ===============");
        System.out.println("Rules:");
        rules.forEach(r -> System.out.println("  " + formatRule(r)));
        System.out.println("Query: " + query);
        System.out.println();
        System.out.println("=========== Rewrite Result ===========");
        System.out.println("Goal: " + result.goal());

        final var rulesWithoutIP0NI0 = result.program().rules().stream()
                .filter(r -> Arrays.stream(r.getBodyAtoms()).noneMatch(a -> a.getPredicate().getName().equals("IP0_NI_0")))
                .toList();

        System.out.println("Rules without IP0_NI_0: ");
        rulesWithoutIP0NI0.forEach(r -> System.out.println("  " + formatRule(r)));
        System.out.println("Rule size without IP0_NI_0: " + rulesWithoutIP0NI0.size());
        System.out.println("Rule size: " + result.program().rules().size());

        System.out.println("Time taken: " + (endTime - startTime) / 1e6 + "ms");
    }

    public static void main(String[] args) {
        final var A = Predicate.create("A", 1);
        final var U = Predicate.create("U", 1);
        final var R = Predicate.create("R", 2);
        final var P = Predicate.create("P", 1);

        final var x1 = Variable.create("x1");
        final var x2 = Variable.create("x2");

        final var x = Variable.create("X");
        final var y = Variable.create("Y");
        final var z = Variable.create("Z");

        // A(x1) -> ∃x2. R(x1, x2)
        // R(x1, x2) -> U(x2)
        // R(x1, x2), U(x2) -> P(x1)
        final List<GTGD> rules = List.of(
                new GTGD(Set.of(Atom.create(A, x1)), Set.of(Atom.create(R, x1, x2))),
                new GTGD(Set.of(Atom.create(R, x1, x2)), Set.of(Atom.create(U, x2))),
                new GTGD(Set.of(Atom.create(R, x1, x2), Atom.create(U, x2)), Set.of(Atom.create(P, x1)))
        );

        // ∃Y. R(X, Y) & R(Y, Z)
        final var query = ConjunctiveQuery.create(
                new Variable[]{x, z},
                new Atom[]{Atom.create(R, x, y), Atom.create(R, y, z)}
        );

        runAndReportTime(rules, query);
    }
}
