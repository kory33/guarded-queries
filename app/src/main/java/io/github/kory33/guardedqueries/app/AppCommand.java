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

    record Rewrite(ConjunctiveQuery query) implements AppCommand {
    }

    record Help() implements AppCommand {
    }
}
