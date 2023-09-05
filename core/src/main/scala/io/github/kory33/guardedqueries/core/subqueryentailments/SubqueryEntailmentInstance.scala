package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, Constant, Variable}

case class SubqueryEntailmentInstance(
  ruleConstantWitnessGuess: Map[Variable, Constant],
  coexistentialVariables: Set[Variable],
  localInstance: LocalInstance,
  localWitnessGuess: Map[Variable, LocalInstanceTerm.LocalName],
  queryConstantEmbedding: BijectiveMap[Constant, LocalInstanceTerm.LocalName]
) {
  def ruleConstantWitnessGuessAsMapToInstanceTerms
    : Map[Variable, LocalInstanceTerm.RuleConstant] =
    ruleConstantWitnessGuess
      .view.mapValues[LocalInstanceTerm.RuleConstant](LocalInstanceTerm.RuleConstant.apply)
      .toMap

  def withLocalInstance(newLocalInstance: LocalInstance): SubqueryEntailmentInstance =
    this.copy(localInstance =
      newLocalInstance
    )

  /**
   * Split this instance into sub-instances using the given commit map.
   * @param relevantSubquery
   *   The subquery described by this instance.
   * @return
   */
  def splitIntoSubInstances(
    commitMap: Map[Variable, LocalInstanceTerm.LocalName]
  )(using relevantSubquery: ConjunctiveQuery): SplitSubqueryEntailmentInstances =
    SplitSubqueryEntailmentInstances(this, commitMap)
}

class SplitSubqueryEntailmentInstances private (
  /**
   * Committed part of the parent subquery entailment instance, which is a variable-free query
   * that mandates the presence of "committed" facts in the current local instance.
   */
  val newlyCommittedPart: List[LocalInstanceTermFact],

  /**
   * Sub-instances induced by the commit map, each of which corresponds to a query-connected
   * component of `parent.coexistentialVariables -- commitMap.keys`
   */
  val subInstances: Set[SubqueryEntailmentInstance]
)

object SplitSubqueryEntailmentInstances {
  import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
  import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions.given

  def apply(
    parentEntailmentInstance: SubqueryEntailmentInstance,
    commitMap: Map[Variable, LocalInstanceTerm.LocalName]
  )(using relevantSubquery: ConjunctiveQuery): SplitSubqueryEntailmentInstances = {
    val extendedLocalWitnessGuess: Map[Variable, LocalInstanceTerm.LocalName] =
      parentEntailmentInstance.localWitnessGuess ++ commitMap

    val extendedGuess: Map[Variable, LocalInstanceTerm] = extendedLocalWitnessGuess ++
      parentEntailmentInstance.ruleConstantWitnessGuessAsMapToInstanceTerms

    new SplitSubqueryEntailmentInstances(
      relevantSubquery.getAtoms
        .filter(_.getVariables.toSet subsetOf extendedGuess.keySet)
        .map(LocalInstanceTermFact.fromAtomWithVariableMap(_, extendedGuess))
        .toList,
      relevantSubquery
        .connectedComponentsOf(
          parentEntailmentInstance.coexistentialVariables -- commitMap.keys
        )
        .map { component =>
          val newNeighbourhood =
            relevantSubquery.strictNeighbourhoodOf(component) --
              parentEntailmentInstance.ruleConstantWitnessGuess.keySet

          val newRelevantSubquery =
            relevantSubquery.subqueryRelevantToVariables(component).get

          SubqueryEntailmentInstance(
            parentEntailmentInstance.ruleConstantWitnessGuess,
            component,
            parentEntailmentInstance.localInstance,
            extendedLocalWitnessGuess.restrictToKeys(newNeighbourhood),
            parentEntailmentInstance.queryConstantEmbedding.restrictToKeys(
              newRelevantSubquery.allConstants
            )
          )
        }
    )
  }
}
