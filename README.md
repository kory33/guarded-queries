# guarded-queries

Rewrite GTGD constraints + conjunctive query into a Datalog program.

## Quick Start

Install Java 17 and [sbt](https://www.scala-sbt.org). Run

```bash
sbt app/assembly

# Run jar file in the same directory, since GSat requires
# a config.properties file in the working directory
java -jar ./app/target/guarded-saturation-app-0.1.0.jar
```

Example interaction:

```
$ java -jar .\app\build\libs\guarded-saturation-0.1.0-all.jar
[guarded-queries app] > help
[guarded-queries app] Available commands:
[guarded-queries app]   add-rule <rule>
[guarded-queries app]   show-rules
[guarded-queries app]   atomic-rewrite
[guarded-queries app]   rewrite <naive|normalizing|dfs-normalizing> <query>
[guarded-queries app]   help
[guarded-queries app]   exit
[guarded-queries app] > add-rule R(x_1, c_1), P(x_1) -> EE y_1. R(c_1, y_1), R(y_1, x_1), P(y_1) 
[guarded-queries app] Registered rule: R(x_1,c_1) ∧ P(x_1) -> ∃y_1. P(y_1) ∧ R(c_1,y_1) ∧ R(y_1,x_1)
[guarded-queries app] > add-rule R(c_1, x_1) -> R(x_1, c_1), P(x_1) 
[guarded-queries app] Registered rule: R(c_1,x_1) -> R(x_1,c_1) ∧ P(x_1)
[guarded-queries app] > rewrite dfs-normalizing EE y. R(c_1, y), R(y, w), R(w, c_3)
[guarded-queries app] Rewriting query:(exists[y]R(c_1,y) & R(y,w) & R(w,c_3))
[guarded-queries app]   using GuardedRuleAndQueryRewriter{saturation=uk.ac.ox.cs.gsat.GSat@22f71333, subqueryEntailmentEnumeration=DFSNormalizingDPTableSEEnumeration{datalogSaturationEngine=io.github.kory33.guardedqueries.core.datalog.saturationengines.NaiveSaturationEngine@13969fbe}}, with registered rules:
[guarded-queries app]   R(x_1,c_1) ∧ P(x_1) -> ∃y_1. P(y_1) ∧ R(c_1,y_1) ∧ R(y_1,x_1)
[guarded-queries app]   R(c_1,x_1) -> R(x_1,c_1) ∧ P(x_1)
Running GSat...

(GSat log...)

[guarded-queries app] Done rewriting query in 2643781300 nanoseconds.
[guarded-queries app] Minimizing the result...
[guarded-queries app] # of subgoal derivation rules in original output: 1895
[guarded-queries app] # of subgoal derivation rules in minimalExactBodyMinimizedRewriting: 23
[guarded-queries app] # of subgoal derivation rules in minimizedRewriting: 7
[guarded-queries app] Done minimizing the result in 98160400 nanoseconds.
[guarded-queries app] Rewritten query:
[guarded-queries app]   Goal atom: IP0_GOAL(w)
[guarded-queries app]   Atomic rewriting part:
[guarded-queries app]     R(GSat_u1,c_1), P(GSat_u1) :- R(c_1,GSat_u1)
[guarded-queries app]     R(GSat_u1,GSat_u2), P(GSat_u1), R(c_1,GSat_u1) :- IP0_NI_0(GSat_u2,GSat_u1)
[guarded-queries app]   Subgoal derivation part:
[guarded-queries app]     IP0_GOAL(w) :- R(w,c_3), IP0_SQ0_GOAL(w)
[guarded-queries app]     IP0_SQ0_SGL_0(x_1) :- R(c_1,x_2), R(x_2,x_1), R(x_2,c_1), P(x_2)
[guarded-queries app]     IP0_SQ0_SGL_0(c_1) :- R(x_2,c_1), R(x_3,c_1), P(x_3)
[guarded-queries app]     IP0_SQ0_GOAL(w) :- IP0_SQ0_SGL_0(w)
[guarded-queries app]     IP0_SQ0_SGL_0(x_1) :- R(x_2,x_1), R(c_1,x_1)
[guarded-queries app]     IP0_SQ0_SGL_0(x_0) :- R(x_0,c_1), P(x_0)
[guarded-queries app]     IP0_SQ0_GOAL(w) :- R(c_1,y), R(y,w)
[guarded-queries app] >
```

## Module Structures

- [`core`](./core): The core library for rewriting GTGD constraints + conjunctive query into a Datalog program.
- [`core-integration-tests`](./core-integration-tests): Integration tests for rewriting algorithms.
- [`app`](./app): A command-line application that uses the core library. Provides an interface as shown in the Quick
  Start section.
- [`formula-parsers`](./formula-parsers): Parsers for GTGD constraints + conjunctive query. Used by app and integration tests.
