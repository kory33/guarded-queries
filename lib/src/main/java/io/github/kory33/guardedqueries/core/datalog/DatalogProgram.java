package io.github.kory33.guardedqueries.core.datalog;

import java.util.Collection;

public record DatalogProgram(Collection<DatalogRule> rules) {
}
