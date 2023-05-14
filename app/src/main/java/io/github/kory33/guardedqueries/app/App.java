package io.github.kory33.guardedqueries.app;

import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult;
import io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine;
import io.github.kory33.guardedqueries.core.fol.DatalogRule;
import io.github.kory33.guardedqueries.core.rewriting.GuardedRuleAndQueryRewriter;
import io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls.DFSNormalizingDPTableSEEnumeration;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimalExactBodyDatalogRuleSet;
import io.github.kory33.guardedqueries.core.subsumption.formula.MinimallyUnifiedDatalogRuleSet;
import uk.ac.ox.cs.gsat.GSat;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Variable;

import java.util.Arrays;
import java.util.stream.Collectors;

public class App {
    private static String formatGTGD(final GTGD rule) {
        final String headString;
        {
            final var head = rule.getHead();
            final String atomsString = Arrays.stream(head.getAtoms())
                    .map(Atom::toString)
                    .collect(Collectors.joining(" ∧ "));

            final var existentialVariables = ImmutableSet.copyOf(rule.getExistential());
            if (existentialVariables.isEmpty()) {
                headString = atomsString;
            } else {
                final var variablesString = existentialVariables.stream()
                        .map(Variable::toString)
                        .collect(Collectors.joining(" ∧ "));
                headString = "∃" + variablesString + ". " + atomsString;
            }
        }

        final String bodyString;
        {
            final var body = rule.getBody();
            bodyString = Arrays.stream(body.getAtoms())
                    .map(Atom::toString)
                    .collect(Collectors.joining(" ∧ "));
        }

        return bodyString + " -> " + headString;
    }

    private static String formatDatalogRule(final DatalogRule rule) {
        final var headString = Arrays.stream(rule.getHead().getAtoms())
                .map(Atom::toString)
                .collect(Collectors.joining(", "));

        final var bodyString = Arrays.stream(rule.getBody().getAtoms())
                .map(Atom::toString)
                .collect(Collectors.joining(", "));

        return headString + " :- " + bodyString;
    }

    private static void log(final String message) {
        System.out.println("[guarded-queries app] " + message);
    }

    private static final GuardedRuleAndQueryRewriter rewriter = new GuardedRuleAndQueryRewriter(
            GSat.getInstance(),
            new DFSNormalizingDPTableSEEnumeration(new NaiveSaturationEngine())
    );

    private static AppCommand parseCommand(final String line) {
        if (line.startsWith("add-rule ")) {
            final var ruleString = line.substring("add-rule ".length());
            final var rule = AppFormulaParsers.gtgd.parse(ruleString);
            return new AppCommand.RegisterRule(rule);
        } else if (line.strip().equals("show-rules")) {
            return new AppCommand.ShowRegisteredRules();
        } else if (line.strip().equals("atomic-rewrite")) {
            return new AppCommand.AtomicRewriteRegisteredRules();
        } else if (line.startsWith("rewrite ")) {
            final var queryString = line.substring("rewrite ".length());
            final var query = AppFormulaParsers.conjunctiveQuery.parse(queryString);
            return new AppCommand.Rewrite(query);
        } else if (line.strip().equals("help")) {
            return new AppCommand.Help();
        } else {
            throw new IllegalArgumentException("Unknown command: " + line);
        }
    }

    private static DatalogRewriteResult minimizeRewriteResultAndLogIntermediateCounts(
            DatalogRewriteResult originalRewriteResult
    ) {
        log("# of subgoal derivation rules in original output: " +
                originalRewriteResult.subgoalAndGoalDerivationRules().rules().size());

        final var minimalExactBodyMinimizedRewriting = originalRewriteResult
                .minimizeSubgoalDerivationRulesUsing(MinimalExactBodyDatalogRuleSet::new);

        log("# of subgoal derivation rules in minimalExactBodyMinimizedRewriting: " +
                minimalExactBodyMinimizedRewriting.subgoalAndGoalDerivationRules().rules().size());

        final var minimizedRewriting = minimalExactBodyMinimizedRewriting
                .minimizeSubgoalDerivationRulesUsing(MinimallyUnifiedDatalogRuleSet::new);

        log("# of subgoal derivation rules in minimizedRewriting: " +
                minimizedRewriting.subgoalAndGoalDerivationRules().rules().size());

        return minimizedRewriting;
    }

    private static AppState runCommandAndReturnNewAppState(final AppState currentState, final AppCommand command) {
        if (command instanceof AppCommand.RegisterRule registerRule) {
            final var rule = registerRule.rule();
            log("Registered rule: " + formatGTGD(rule));
            return currentState.registerRule(rule);
        } else if (command instanceof AppCommand.ShowRegisteredRules) {
            log("Registered rules:");
            currentState.registeredRules().forEach(rule -> log("  " + formatGTGD(rule)));
            return currentState;
        } else if (command instanceof AppCommand.AtomicRewriteRegisteredRules) {
            log("Computing atomic rewriting of registered rules.");
            log("Registered rules:");
            currentState.registeredRules().forEach(rule -> log("  " + formatGTGD(rule)));

            final var rewrittenRules = GSat.getInstance().run(currentState.registeredRulesAsDependencies());
            log("Done computing atomic rewriting of registered rules");
            log("Rewritten rules:");
            rewrittenRules.forEach(rule -> log("  " + formatGTGD(rule)));
        } else if (command instanceof AppCommand.Rewrite rewrite) {
            log("Rewriting query:" + rewrite.query() + " under the following rules:");
            currentState.registeredRules().forEach(rule -> log("  " + formatGTGD(rule)));

            final var beginRewriteNanoTime = System.nanoTime();
            final var rewriteResult = rewriter.rewrite(currentState.registeredRules(), rewrite.query());
            final var rewriteTimeNanos = System.nanoTime() - beginRewriteNanoTime;
            log("Done rewriting query in " + rewriteTimeNanos + " nanoseconds.");
            log("Minimizing the result...");

            final var beginMinimizeNanoTime = System.nanoTime();
            final var minimizedRewriteResult = minimizeRewriteResultAndLogIntermediateCounts(rewriteResult);
            final var minimizeTimeNanos = System.nanoTime() - beginMinimizeNanoTime;
            log("Done minimizing the result in " + minimizeTimeNanos + " nanoseconds.");

            log("Rewritten query:");
            log("  Goal atom: " + minimizedRewriteResult.goal());
            log("  Atomic rewriting part:");
            minimizedRewriteResult.inputRuleSaturationRules().rules().forEach(rule -> log("    " + formatDatalogRule(rule)));
            log("  Subgoal derivation part:");
            minimizedRewriteResult.subgoalAndGoalDerivationRules().rules().forEach(rule -> log("    " + formatDatalogRule(rule)));
        } else if (command instanceof AppCommand.Help) {
            log("Available commands:");
            log("  add-rule <rule>");
            log("  show-rules");
            log("  atomic-rewrite");
            log("  rewrite <query>");
            log("  help");
            log("  exit");
        } else {
            throw new IllegalStateException("Unknown command: " + command);
        }

        return currentState;
    }

    public static void main(String[] args) {
        var appState = new AppState();
        final var scanner = new java.util.Scanner(System.in);

        while (true) {
            System.out.print("[guarded-queries app] > ");
            final var nextLine = scanner.nextLine();

            if (nextLine.strip().equals("exit")) {
                break;
            } else if (nextLine.strip().equals("")) {
                continue;
            }

            try {
                var command = parseCommand(nextLine);
                appState = runCommandAndReturnNewAppState(appState, command);
            } catch (Exception e) {
                log("Error: " + e);
            }
        }
    }
}
