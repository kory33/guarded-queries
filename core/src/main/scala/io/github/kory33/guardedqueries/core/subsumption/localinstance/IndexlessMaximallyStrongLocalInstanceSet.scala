package io.github.kory33.guardedqueries.core.subsumption.localinstance
import io.github.kory33.guardedqueries.core.formalinstance.QueryLikeInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm
}
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subsumption.localinstance.MaximallyStrongLocalInstanceSet.AddResult

import scala.collection.mutable

/**
 * An implementation of [[MaximallyStrongLocalInstanceSet]] which does not use any index.
 */
class IndexlessMaximallyStrongLocalInstanceSet(
  joinAlgorithm: NaturalJoinAlgorithm[LocalName, LocalInstanceTerm, LocalInstance],
  override val localNamesToFix: Set[LocalInstanceTerm.LocalName]
) extends MaximallyStrongLocalInstanceSet {
  private val instancesKnownToBeMaximallyStrongSoFar: mutable.Set[LocalInstance] =
    mutable.HashSet.empty[LocalInstance]

  extension (instance: LocalInstance)
    private def asStrongAs(another: LocalInstance): Boolean = {
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

  private def isWeakerThanSomeInstance(instance: LocalInstance) =
    instancesKnownToBeMaximallyStrongSoFar.exists(_ asStrongAs instance)

  override def add(instance: LocalInstance): AddResult = {
    if (!isWeakerThanSomeInstance(instance)) {
      instancesKnownToBeMaximallyStrongSoFar.filterInPlace(rule => !(instance asStrongAs rule))
      instancesKnownToBeMaximallyStrongSoFar.add(instance)
      AddResult.Added
    } else {
      AddResult.WeakerThanAnotherLocalInstance
    }
  }

  override def getMaximalLocalInstances: Set[LocalInstance] =
    instancesKnownToBeMaximallyStrongSoFar.toSet
}
