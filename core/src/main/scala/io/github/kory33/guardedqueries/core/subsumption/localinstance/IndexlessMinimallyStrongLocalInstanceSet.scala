package io.github.kory33.guardedqueries.core.subsumption.localinstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstance
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subsumption.localinstance.MinimallyStrongLocalInstanceSet.AddResult

import scala.collection.mutable

/**
 * An implementation of [[MinimallyStrongLocalInstanceSet]] which does not use any index.
 */
class IndexlessMinimallyStrongLocalInstanceSet(
  override val localNamesToFix: Set[LocalInstanceTerm.LocalName]
)(
  using joinAlgorithm: NaturalJoinAlgorithm[LocalName, LocalInstanceTerm, LocalInstance]
) extends MinimallyStrongLocalInstanceSet {
  private val instancesKnownToBeMinimallyStrongSoFar: mutable.Set[LocalInstance] =
    mutable.HashSet.empty[LocalInstance]

  private val strengthRelation = LocalInstanceStrengthRelation(localNamesToFix)
  import strengthRelation.given

  private def isAsStrongAsSomeInstance(instance: LocalInstance) =
    instancesKnownToBeMinimallyStrongSoFar.exists(_ asWeakAs instance)

  override def add(instanceToAdd: LocalInstance): AddResult = {
    if (!isAsStrongAsSomeInstance(instanceToAdd)) {
      instancesKnownToBeMinimallyStrongSoFar.filterInPlace(instance =>
        !(instanceToAdd asWeakAs instance)
      )
      instancesKnownToBeMinimallyStrongSoFar.add(instanceToAdd)
      AddResult.Added
    } else {
      AddResult.StrongerThanAnotherLocalInstance
    }
  }

  override def getMinimalLocalInstances: Set[LocalInstance] =
    instancesKnownToBeMinimallyStrongSoFar.toSet
}
