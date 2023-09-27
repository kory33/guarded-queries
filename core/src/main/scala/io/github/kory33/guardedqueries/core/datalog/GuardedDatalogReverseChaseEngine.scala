package io.github.kory33.guardedqueries.core.datalog

import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm
}

/**
 * A trait representing a reverse chase engine for guarded Datalog.
 *
 * Let ≲ be the "weaker than" relation on local instances, as described in
 * [[io.github.kory33.guardedqueries.core.subsumption.localinstance.MinimallyStrongLocalInstanceSet]].
 *
 * The [[reverseFullChase]] method returns a finite collection of all (≲-isomorphism-classes of)
 * ≲-minimal local instances `I` that satisfies
 *   - `instance ≲ saturate(program, I)`, where `saturate` is
 *     [[DatalogSaturationEngine.saturateInstance]], and
 *   - the width of `I` (number of local names active in `I`) is at most
 *     `instanceWidthUpperLimit`.
 */
trait GuardedDatalogReverseChaseEngine {
  // TODO (Question):
  //   Is it OK to take `instanceWidthUpperLimit` to be the maximum arity of predicates
  //   appearing in `program` and omit this parameter?
  def reverseFullChase(
    localNamesToFix: Set[LocalName],
    program: GuardedDatalogProgram,
    instanceWidthUpperLimit: Int,
    instance: LocalInstance
  ): Iterable[LocalInstance]
}
