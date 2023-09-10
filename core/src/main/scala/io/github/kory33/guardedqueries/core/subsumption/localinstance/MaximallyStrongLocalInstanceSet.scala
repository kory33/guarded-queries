package io.github.kory33.guardedqueries.core.subsumption.localinstance

import io.github.kory33.guardedqueries.core.formalinstance.QueryLikeInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm
}
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName

/**
 * An interface to objects that can keep track of a set of local instances which are "maximal"
 * with respect to the following "strength relation":
 *
 * Fix [[localNamesToFix]] and define a binary relation `R` on [[LocalInstance]] as follows:
 * `instance1 R instance2` if and only if there exists a map `s: LocalName => LocalInstanceTerm`
 * such that
 *   - `s(instance2) subsetOf instance1`, and
 *   - `s` is the identity map on [[localNamesToFix]].
 *
 * The relation `R` so defined is a preorder on [[LocalInstance]]. When we have a relation
 * `instance1 R instance2`, we say that "`instance2` is as strong as `instance1` (with respect
 * to `localNamesToFix`)".
 *
 * Local instances can also be thought of as "preconditions" to witness a certain existential
 * query. Under this formulation, the smaller and the more "un-unified" the local instance is,
 * the more weaker the assumption is. This observation justifies the name "strength relation",
 * in that the local instances acting as weaker preconditions are more useful, hence "stronger".
 *
 * Objects implementing [[MaximallyStrongLocalInstanceSet]] are useful in finding the maximally
 * strong local instances stronger than a given local instance.
 */
trait MaximallyStrongLocalInstanceSet {
  val localNamesToFix: Set[LocalName]

  /**
   * Add a local instance to this set.
   *
   * The method should check that the local instance is not weaker than any of the local
   * instances already in the set. If the instance is weaker, the method should return
   * [[AddResult.WeakerThanAnotherLocalInstance]]. If the instance is not weaker, the method
   * should remove all the local instances in the set that are weaker than the given instance,
   * add the given instance to the set, and return [[AddResult.Added]].
   */
  def add(localInstance: LocalInstance): MaximallyStrongLocalInstanceSet.AddResult

  /**
   * Get the set of local instances currently in this set.
   */
  def getMaximalLocalInstances: Set[LocalInstance]
}

object MaximallyStrongLocalInstanceSet {
  trait Factory {
    def newSet(localNamesToFix: Set[LocalName]): MaximallyStrongLocalInstanceSet
  }

  enum AddResult:
    case Added
    case WeakerThanAnotherLocalInstance
}

/**
 * The implementation of the "strength relation" on local instances.
 */
case class LocalInstanceStrengthRelation(
  localNamesToFix: Set[LocalName]
) {
  given Extension: AnyRef with
    extension (instance: LocalInstance)
      def asStrongAs(another: LocalInstance)(
        using joinAlgorithm: NaturalJoinAlgorithm[LocalName, LocalInstanceTerm, LocalInstance]
      ): Boolean = {
        // Recall that we have to find a map `s: LocalName => LocalInstanceTerm` such that
        //   - `s(instance) subsetOf another`, and
        //   - `s` is the identity map on `localNamesToFix`.
        // This corresponds to checking the non-emptiness of the query `query` on `another`, where
        // `query` is the existential-free conjunctive query whose conjuncts are formal facts from `instance`,
        // where local names in `localNamesToFix` and rule constants are treated as query constants
        // and other local names are treated as query variables.
        val query: QueryLikeInstance[LocalName, LocalInstanceTerm] = instance.map {
          case name: LocalName =>
            if (localNamesToFix.contains(name)) {
              Right(name)
            } else {
              Left(name)
            }
          case ruleConstant: LocalInstanceTerm.RuleConstant =>
            Right(ruleConstant)
        }

        joinAlgorithm.join(query, another).nonEmpty
      }
}
