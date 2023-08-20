package io.github.kory33.guardedqueries.app;

import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery;

public sealed interface AppCommand {
    record RegisterRule(GTGD rule) implements AppCommand {
    }

    record ShowRegisteredRules() implements AppCommand {
    }

    record AtomicRewriteRegisteredRules() implements AppCommand {
    }

    record Rewrite(ConjunctiveQuery query, EnumerationImplChoice implChoice) implements AppCommand {
        public sealed interface EnumerationImplChoice {
            record Naive() implements EnumerationImplChoice {
            }

            record Normalizing() implements EnumerationImplChoice {
            }

            record DFSNormalizing() implements EnumerationImplChoice {
            }
        }
    }

    record Help() implements AppCommand {
    }
}
