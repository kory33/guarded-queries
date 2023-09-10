package io.github.kory33.guardedqueries.core.subsumption.localinstance
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
  override val localNamesToFix: Set[LocalInstanceTerm.LocalName]
)(
  using joinAlgorithm: NaturalJoinAlgorithm[LocalName, LocalInstanceTerm, LocalInstance]
) extends MaximallyStrongLocalInstanceSet {
  private val instancesKnownToBeMaximallyStrongSoFar: mutable.Set[LocalInstance] =
    mutable.HashSet.empty[LocalInstance]

  private val strengthRelation = LocalInstanceStrengthRelation(localNamesToFix)
  import strengthRelation.given

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
