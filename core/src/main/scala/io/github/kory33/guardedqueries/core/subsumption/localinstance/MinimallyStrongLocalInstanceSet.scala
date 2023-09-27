package io.github.kory33.guardedqueries.core.subsumption.localinstance

import io.github.kory33.guardedqueries.core.formalinstance.QueryLikeInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm
}
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName

/**
 * An interface to objects that can keep track of a set of local instances which are "minimal"
 * with respect to the following "strength relation":
 *
 * We fix [[localNamesToFix]] and define a binary relation `≲` on [[LocalInstance]] as follows:
 * `instance1 ≲ instance2` if and only if there exists a map `s: LocalName => LocalInstanceTerm`
 * such that
 *   - `s(instance1) ⊆ instance2`, and
 *   - `s` is the identity map on [[localNamesToFix]].
 *
 * The relation `≲` so defined is a preorder on [[LocalInstance]]. When we have a relation
 * `instance1 ≲ instance2`, we say that "`instance2` is stronger than `instance1` (with respect
 * to `localNamesToFix`)", or that "`instance1` is weaker than `instance2`". Here, the notion of
 * being "weaker than" or "stronger than" is not strict.
 *
 * It follows that if `instance1 ≲ instance2`, then the shortcut chase-tree of `instance1` can
 * be homomorphically mapped to `instance2`.
 *
 * Objects implementing [[MinimallyStrongLocalInstanceSet]] are useful in finding the set of
 * minimally strong local instances from the given set of local instances.
 */
trait MinimallyStrongLocalInstanceSet {
  val localNamesToFix: Set[LocalName]

  /**
   * Add a local instance to this set.
   *
   * The method should check if there is a local instance in that set that is as weak as the
   * given [[localInstance]]. If there is such an instance, the method should return
   * [[AddResult.StrongerThanAnotherLocalInstance]]. Otherwise, the method should remove all the
   * local instances in the set that are stronger than [[localInstance]], add [[localInstance]]
   * to the set, and return [[AddResult.Added]].
   */
  def add(localInstance: LocalInstance): MinimallyStrongLocalInstanceSet.AddResult

  /**
   * Get the set of local instances currently in this set.
   */
  def getMinimalLocalInstances: Set[LocalInstance]
}

object MinimallyStrongLocalInstanceSet {
  trait Factory {
    def newSet(localNamesToFix: Set[LocalName]): MinimallyStrongLocalInstanceSet
  }

  enum AddResult:
    case Added
    case StrongerThanAnotherLocalInstance
}

/**
 * The implementation of the "strength relation" on local instances.
 */
case class LocalInstanceStrengthRelation(
  localNamesToFix: Set[LocalName]
) {
  given Extension: AnyRef with
    extension (instance: LocalInstance)
      def asWeakAs(another: LocalInstance)(
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
