package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable

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
      .view.mapValues(LocalInstanceTerm.RuleConstant.apply)
      .toMap

  def withLocalInstance(newLocalInstance: LocalInstance): SubqueryEntailmentInstance =
    this.copy(localInstance =
      newLocalInstance
    )
}
