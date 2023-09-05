package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.{LocalInstance, LocalInstanceTerm}

/**
 * A trait representing a reverse chase engine for guarded Datalog.
 *
 * Fix `localNamesToFix` and define a binary relation `R` on `LocalInstance` as follows:
 * `instance1 R instance2` if and only if there exists a map `s: TA => TA` such that
 *   - `s(instance2) subsetOf instance1`, and
 *   - `s` is the identity map on `localNamesToFix`.
 *
 * The relation `R` so defined is a preorder on `LocalInstance`. When we have a relation
 * `instance1 R instance2`, we say that "`instance2` is as strong as `instance1` (with respect
 * to `localNamesToFix`)".
 *
 * The [[reverseChase]] method returns a finite collection of all (`R`-isomorphism-classes of)
 * `R`-maximal local instances `I` that satisfies `instance R saturate(program, I)`, where
 * `saturate` is [[DatalogSaturationEngine.saturateInstance]].
 */
trait GuardedDatalogReverseChaseEngine {
  def reverseChase(
    localNamesToFix: Set[LocalName],
    program: GuardedDatalogProgram,
    instance: LocalInstance
  ): Iterable[LocalInstance]
}
